<?php

namespace MediaWiki\Extension\Bucket\Query;

use InvalidArgumentException;
use MediaWiki\Extension\Bucket\BucketDatabase;
use MediaWiki\Extension\Bucket\Exception\BucketException;
use MediaWiki\Extension\Bucket\Exception\QueryException;
use MediaWiki\Extension\Bucket\Schema\BucketSchema;
use MediaWiki\Linker\LinksMigration;
use MediaWiki\MediaWikiServices;
use MediaWiki\Title\Title;
use Wikimedia\Rdbms\IDatabase;
use Wikimedia\Rdbms\IExpression;
use Wikimedia\Rdbms\SelectQueryBuilder;

/**
 * @property BucketSchema[] $schemas - The schema objects, populated from self::$schemaCache
 * @property Join[] $joins
 * @property Selector[] $selects
 * @property FieldSelector[] $orderByFields
 */
class BucketQuery {
	public const MAX_LIMIT = 5000;
	public const DEFAULT_LIMIT = 500;

	/**
	 * Caches schemas that are read from the database, so multiple queries on one page can reuse them.
	 */
	private static array $schemaCache = [];

	private array $schemas = [];
	private array $categories = [];
	private BucketSchema $primarySchema;
	private array $joins = [];
	private array $selects = [];
	private ?QueryNode $where = null;
	private int $limit = self::DEFAULT_LIMIT;
	private int $offset = 0;
	private array $orderByFields = [];
	private ?Selector $userOrderByField = null;
	private string $orderByDirection = 'ASC';
	private int $categoryCount = 0;
	public bool $useLinkTarget = false;

	/**
	 * @param mixed $condition
	 * @return bool
	 */
	private static function isNot( $condition ) {
		return is_array( $condition )
		&& isset( $condition['op'] )
		&& $condition['op'] === 'NOT'
		&& isset( $condition['operand'] );
	}

	/**
	 * @param mixed $condition
	 * @return bool
	 */
	private static function isOrAnd( $condition ) {
		return is_array( $condition )
		&& isset( $condition['op'] )
		&& ( $condition['op'] === 'OR' || $condition['op'] === 'AND' )
		&& isset( $condition['operands'] )
		&& is_array( $condition['operands'] );
	}

	/**
	 * @param string $fieldName
	 * @return bool
	 */
	public static function isCategory( $fieldName ): bool {
		$categoryPrefix = MediaWikiServices::getInstance()->getContentLanguage()->getFormattedNsText( NS_CATEGORY );
		$categoryPrefix = strtolower( $categoryPrefix );
		return str_starts_with( strtolower( trim( $fieldName ) ), $categoryPrefix . ':' );
	}

	public static function clearCache() {
		// We must clear the schemaCache when we begin a new page because the DB transaction
		// is committed at the end of each page, which means the cache may be invalid.
		self::$schemaCache = [];
	}

	/**
	 * @param array $data
	 */
	public function __construct( array $data ) {
		// Support for the categorylinks migration.
		if ( array_key_exists( 'categorylinks', LinksMigration::$mapping ) ) {
			$linksMigration = MediaWikiServices::getInstance()->getLinksMigration();
			$queryInfo = $linksMigration->getQueryInfo( 'categorylinks' );
			$this->useLinkTarget = in_array( 'linktarget', $queryInfo['tables'], true );
		} else {
			$this->useLinkTarget = false;
		}
		// Ensure schema cache is populated with all used buckets
		$neededSchemas = [];
		if ( !isset( self::$schemaCache[$data['bucketName']] ) ) {
			$neededSchemas[] = $data['bucketName'];
		}
		foreach ( $data['joins'] as $join ) {
			if ( !isset( self::$schemaCache[$join['bucketName']] ) ) {
				$neededSchemas[] = $join['bucketName'];
			}
		}
		// Populate the schema cache with missing schemas
		if ( $neededSchemas ) {
			$dbw = BucketDatabase::getDB();
			$res = $dbw->newSelectQueryBuilder()
				->from( 'bucket_schemas' )
				->select( [ 'bucket_name', 'schema_json' ] )
				->lockInShareMode()
				->where( [ 'bucket_name' => $neededSchemas ] )
				->caller( __METHOD__ )
				->fetchResultSet();
			// Flip array so that we can unset based on name
			$neededSchemas = array_flip( $neededSchemas );
			foreach ( $res as $row ) {
				self::$schemaCache[$row->bucket_name] = json_decode( $row->schema_json, true );
				// Indicate we have found this bucket
				unset( $neededSchemas[$row->bucket_name] );
			}
		}

		// Check that all needed schemas were retrieved
		if ( count( $neededSchemas ) > 0 ) {
			throw new QueryException( wfMessage( 'bucket-no-exist', array_key_first( $neededSchemas ) ) );
		}

		$primarySchema = new BucketSchema( $data['bucketName'], self::$schemaCache[$data['bucketName']] );
		$this->schemas[$primarySchema->getName()] = $primarySchema;
		$this->primarySchema = $primarySchema;
		$this->orderByFields[] = new FieldSelector( '_page_id', $this );
		$this->orderByFields[] = new FieldSelector( '_index', $this );

		foreach ( $data['joins'] as $join ) {
			if ( !is_array( $join['cond'] ) || count( $join['cond'] ) !== 2 ) {
				throw new QueryException( wfMessage( 'bucket-query-invalid-join', json_encode( $join ) ) );
			}
			$joinTable = $join['bucketName'];
			'@phan-var string $joinTable';
			$field1 = $join['cond'][0];
			$field2 = $join['cond'][1];
			$schema = self::$schemaCache[$join['bucketName']];

			if ( isset( $this->joins[$joinTable] ) ) {
				throw new QueryException( wfMessage( 'bucket-query-duplicate-join', $joinTable ) );
			}

			$joinTableSchema = new BucketSchema( $joinTable, $schema );
			$this->schemas[$joinTableSchema->getName()] = $joinTableSchema;
			$join = new BucketJoin( $joinTableSchema, $field1, $field2, $this );
			$this->joins[$joinTable] = $join;
			$this->orderByFields[] = new FieldSelector( $joinTableSchema->getName() . '._page_id', $this );
			$this->orderByFields[] = new FieldSelector( $joinTableSchema->getName() . '._index', $this );
		}

		// Parse selects
		if ( empty( $data['selects'] ) ) {
			throw new QueryException( wfMessage( 'bucket-query-select-empty' ) );
		}
		foreach ( $data['selects'] as $select ) {
			if ( self::isCategory( $select ) ) {
				$this->selects[] = new CategorySelector( $select, $this );
				$this->addCategoryJoin( $select );
			} else {
				$this->selects[] = new FieldSelector( $select, $this );
			}
		}

		// Parse wheres
		if ( count( $data['wheres']['operands'] ) > 0 ) {
			$this->where = $this->parseWhere( $data['wheres'] );
		}

		// Parse other options
		if ( isset( $data['limit_arg'] ) && is_int( $data['limit_arg'] ) && $data['limit_arg'] > 0 ) {
			$this->limit = min( $data['limit_arg'], self::MAX_LIMIT );
		}

		if ( isset( $data['offset_arg'] ) && is_int( $data['offset_arg'] ) && $data['offset_arg'] > 0 ) {
			$this->offset = $data['offset_arg'];
		}

		if ( isset( $data['orderBy'] ) ) {
			$orderByField = $data['orderBy']['fieldName'];
			$isSelected = false;
			if ( self::isCategory( $orderByField ) ) {
				$this->userOrderByField = new CategorySelector( $orderByField, $this );
			} else {
				$this->userOrderByField = new FieldSelector( $orderByField, $this );
			}
			foreach ( $this->selects as $select ) {
				if ( $select == $this->userOrderByField ) {
					$isSelected = true;
					break;
				}
			}
			if ( !$isSelected ) {
				throw new QueryException( wfMessage( 'bucket-query-order-by-must-select', $orderByField ) );
			}

			$direction = $data['orderBy']['direction'];
			if ( $direction !== 'ASC' && $direction !== 'DESC' ) {
				throw new QueryException( wfMessage( 'bucket-query-order-by-direction', $direction ) );
			}
			$this->orderByDirection = $direction;
		}
	}

	private function addCategoryJoin( string $category ) {
		if ( !isset( $this->categories[$category] ) ) {
			$categoryTitle = Title::newFromText( $category, NS_CATEGORY );
			if ( !$categoryTitle ) {
				throw new QueryException( wfMessage( 'bucket-query-invalid-category-name', $category ) );
			}

			$categoryAlias = 'category' . $this->categoryCount++;
			$this->categories[$category] = $categoryAlias;
			if ( $this->useLinkTarget ) {
				$this->joins[] = new LinkTargetJoin( $category, $this );
			}
			$this->joins[] = new CategoryJoin( $category, $this );
		}
	}

	public function getCategoryAlias( string $category ): string {
		return $this->categories[$category];
	}

	public function getLinkTargetAlias( string $category ): string {
		return $this->categories[$category] . '_lt';
	}

	public function getPrimaryBucket(): BucketSchema {
		return $this->primarySchema;
	}

	/**
	 * @return Selector[]
	 */
	public function getFields(): array {
		return $this->selects;
	}

	/**
	 * @return BucketSchema[]
	 */
	public function getUsedBuckets(): array {
		return $this->schemas;
	}

	private function getWhereSQL( IDatabase $dbw ): IExpression|array {
		if ( $this->where !== null ) {
			try {
				return $this->where->getWhereSQL( $dbw );
			} catch ( InvalidArgumentException $e ) {
				// The rdbms query builder throws InvalidArgumentException for input it doesn't like.
				// We shouldn't be passing anything that it doesn't like, but better to catch here
				// so we can turn it into a non fatal BucketException
				throw new BucketException( wfMessage( $e->getMessage() ) );
			}
		} else {
			return [];
		}
	}

	public function getSelectQueryBuilder(): SelectQueryBuilder {
		$dbw = BucketDatabase::getDB();
		$builder = $dbw->newSelectQueryBuilder()
			->from( BucketDatabase::getBucketTableName( $this->getPrimaryBucket()->getName() ) )
			->caller( __METHOD__ );

		$field_count = 0;
		foreach ( $this->selects as $selector ) {
			$uniqueAlias = str_replace( '.', '_', $selector->getUnsafe() ) . $field_count++;
			$builder->field( $selector->getSelectSQL( $dbw ), $uniqueAlias );
		}

		// MySQL can only use 61 tables in a join (initial table + 60 joins)
		if ( count( $this->joins ) > 60 ) {
			throw new QueryException( wfMessage( 'bucket-query-join-limit', count( $this->joins ), 60 ) );
		}

		foreach ( $this->joins as $join ) {
			$builder->leftJoin( $join->getJoinTable( $dbw ), $join->getAlias(), $join->getSQL( $dbw ) );
		}

		$builder->where( $this->getWhereSQL( $dbw ) );
		$builder->option( 'LIMIT', $this->limit );
		$builder->option( 'OFFSET', $this->offset );

		$config = MediaWikiServices::getInstance()->getMainConfig();
		$builder->setMaxExecutionTime( $config->get( 'BucketMaxQueryExecutionTime' ) );

		$orderByStrings = [];
		if ( $this->userOrderByField !== null ) {
			$orderByStrings[] = $this->userOrderByField->getSafe( $dbw );
		}
		foreach ( $this->orderByFields as $field ) {
			$orderByStrings[] = $field->getSafe( $dbw );
		}
		$builder->orderBy( $orderByStrings, $this->orderByDirection );

		return $builder;
	}

	private function parseWhere( array|string $condition ): QueryNode {
		if ( self::isOrAnd( $condition ) ) {
			$children = [];
			$subqueryChildren = [];
			$subquerySelectors = [];
			foreach ( $condition['operands'] as $operand ) {
				$child = $this->parseWhere( $operand );
				if ( $child instanceof ComparisonConditionNode ) {
					$selector = $child->getSelector();
					if ( $selector instanceof FieldSelector && $selector->getFieldSchema()->getRepeated() ) {
						$fieldName = $selector->getFieldSchema()->getFieldName();
						if ( !isset( $subqueryChildren[$fieldName] ) ) {
							$subqueryChildren[$fieldName] = [];
							$subquerySelectors[$fieldName] = $selector;
						}
						$subqueryChildren[$fieldName][] = $child;
						$child->getSelector()->setSubquery( true );
						continue;
					}
				}
				$children[] = $child;
			}
			foreach ( $subqueryChildren as $fieldName => $group ) {
				if ( $condition['op'] === 'OR' ) {
					$newNode = new OrNode( $group );
				} else {
					$newNode = new AndNode( $group );
				}
				$children[] = new SubqueryNode( $subquerySelectors[$fieldName], $newNode );
			}
			if ( $condition['op'] === 'OR' ) {
				return new OrNode( $children );
			} else {
				return new AndNode( $children );
			}
		}
		if ( self::isNot( $condition ) ) {
			return new NotNode( $this->parseWhere( $condition['operand'] ) );
		}
		if ( is_array( $condition ) && isset( $condition[0] ) && isset( $condition[1] ) && isset( $condition[2] ) ) {
			$selector = new FieldSelector( $condition[0], $this );
			$op = new Operator( $condition[1] );
			// Lua cannot store nil, so we convert the null string into real null value.
			if ( $condition[2] === '&&NULL&&' ) {
				$value = new Value( null );
			} else {
				$value = new Value( $condition[2] );
			}
			return new ComparisonConditionNode( $selector, $op, $value );
		}
		if ( ( is_string( $condition ) && self::isCategory( $condition ) ) ||
			( is_array( $condition ) && self::isCategory( $condition[0] ) ) ) {
			if ( is_array( $condition ) ) {
				$condition = $condition[0];
			}
			$selector = new CategorySelector( $condition, $this );
			$this->addCategoryJoin( $condition );
			$op = new Operator( '!=' );
			$value = new Value( null );
			return new ComparisonConditionNode( $selector, $op, $value );
		}
		throw new QueryException( wfMessage( 'bucket-query-where-confused', json_encode( $condition ) ) );
	}
}
