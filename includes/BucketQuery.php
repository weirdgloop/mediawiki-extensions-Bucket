<?php

namespace MediaWiki\Extension\Bucket;

use MediaWiki\MediaWikiServices;
use MediaWiki\Title\Title;
use Wikimedia\Rdbms\IDatabase;
use Wikimedia\Rdbms\SelectQueryBuilder;

/**
 * @property BucketSchema[] $schemas - The schema objects, populated from self::$schemaCache
 * @property Join[] $joins
 * @property Selector[] $selects
 */
class BucketQuery {
	public const MAX_LIMIT = 5000;
	public const DEFAULT_LIMIT = 500;
	// Caches schemas that are read from the database, so multiple queries on one page can reuse them.
	private static $schemaCache = [];

	private array $schemas = [];
	private array $categories = [];
	private BucketSchema $primarySchema;
	private array $joins = [];
	private array $selects = [];
	private QueryNode $where;
	private int $limit = self::DEFAULT_LIMIT;
	private int $offset = 0;
	private ?Selector $orderByField = null;
	private ?string $orderByDirection = null;
	private int $categoryCount = 0;

	public static function isNot( $condition ) {
		return is_array( $condition )
		&& isset( $condition['op'] )
		&& $condition['op'] == 'NOT'
		&& isset( $condition['operand'] );
	}

	public static function isOrAnd( $condition ) {
		return is_array( $condition )
		&& isset( $condition['op'] )
		&& ( $condition['op'] === 'OR' || $condition['op'] === 'AND' )
		&& isset( $condition['operands'] )
		&& is_array( $condition['operands'] );
	}

	public static function isCategory( $fieldName ): bool {
		return substr( strtolower( trim( $fieldName ) ), 0, 9 ) == 'category:';
	}

	public function __construct( $data ) {
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
		if ( !empty( $neededSchemas ) ) {
			$dbw = Bucket::getDB();
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
				unset( $neededSchemas[$row->bucket_name] ); // Indicate we have found this bucket
			}
		}

		// Check that all needed schemas were retrieved
		if ( count( $neededSchemas ) > 0 ) {
			throw new QueryException( wfMessage( 'bucket-no-exist', array_key_first( $neededSchemas ) ) );
		}

		$primarySchema = new BucketSchema( $data['bucketName'], self::$schemaCache[$data['bucketName']] );
		$this->schemas[$primarySchema->getName()] = $primarySchema;
		$this->primarySchema = $primarySchema;

		foreach ( $data['joins'] as $join ) {
			if ( !is_array( $join['cond'] ) || !count( $join['cond'] ) == 2 ) {
				throw new QueryException( wfMessage( 'bucket-query-invalid-join', json_encode( $join ) ) );
			}
			$joinTable = $join['bucketName'];
			$field1 = $join['cond'][0];
			$field2 = $join['cond'][1];
			$schema = self::$schemaCache[$join['bucketName']];

			$joinTableSchema = new BucketSchema( $joinTable, $schema );
			$this->schemas[$joinTableSchema->getName()] = $joinTableSchema;
			$join = new BucketJoin( $joinTableSchema, $field1, $field2, $this );
			$joinedTableName = $joinTableSchema->getName();
			if ( isset( $joins[$joinedTableName] ) ) {
				throw new QueryException( wfMessage( 'bucket-select-duplicate-join', $joinedTableName ) );
			}
			$this->joins[] = $join;
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
		$where = $this->parseWhere( $data['wheres'] );
		$this->where = $where;

		// Parse other options
		if ( isset( $data['limit'] ) && is_int( $data['limit'] ) && $data['limit'] > 0 ) {
			$this->limit = min( $data['limit'], self::MAX_LIMIT );
		}

		if ( isset( $data['offset'] ) && is_int( $data['offset'] ) && $data['offset'] > 0 ) {
			$this->offset = $data['offset'];
		}

		if ( isset( $data['orderBy'] ) ) {
			$orderByField = $data['orderBy']['fieldName'];
			$isSelected = false;
			if ( self::isCategory( $orderByField ) ) {
				$this->orderByField = new CategorySelector( $orderByField, $this );
			} else {
				$this->orderByField = new FieldSelector( $orderByField, $this );
			}
			foreach ( $this->selects as $select ) {
				if ( $select == $this->orderByField ) {
					$isSelected = true;
					break;
				}
			}
			if ( !$isSelected ) {
				throw new QueryException( wfMessage( 'bucket-query-order-by-must-select', $orderByField ) );
			}

			$direction = $data['orderBy']['direction'];
			if ( $direction != 'ASC' && $direction != 'DESC' ) {
				throw new QueryException( wfMessage( 'bucket-query-order-by-direction', $direction ) );
			}
			$this->orderByDirection = $direction;
		}
	}

	private function addCategoryJoin( string $category ) {
		if ( !isset( $this->categories[$category] ) ) {
			$categoryAlias = 'category' . $this->categoryCount++;
			$this->categories[$category] = $categoryAlias;
			$this->joins[] = new CategoryJoin( $category, $this );
		}
	}

	function getCategoryAlias( string $category ): string {
		return $this->categories[$category];
	}

	function getPrimaryBucket(): BucketSchema {
		return $this->primarySchema;
	}

	/**
	 * @return Selector[]
	 */
	function getFields(): array {
		return $this->selects;
	}

	/**
	 * @return BucketSchema[]
	 */
	function getUsedBuckets(): array {
		return $this->schemas;
	}

	private function getWhereSQL(): string|array {
		$wheres = $this->where->getWhereSQL();
		if ( $wheres == '' ) {
			return []; // Select query builder doesn't like an empty string or null.
		}
		return $wheres;
	}

	public function getSelectQueryBuilder(): SelectQueryBuilder {
		$dbw = Bucket::getDB();
		$builder = $dbw->newSelectQueryBuilder()
			->from( Bucket::getBucketTableName( $this->getPrimaryBucket()->getName() ) )
			->caller( __METHOD__ );

		foreach ( $this->selects as $selector ) {
			$builder->field( $selector->getSelectSQL( $dbw ) );
		}

		foreach ( $this->joins as $join ) {
			if ( $join instanceof CategoryJoin ) {
				$builder->leftJoin( 'categorylinks', $join->getAlias(), $join->getSQL( $dbw ) );
			} else {
				$builder->leftJoin( $join->getAlias( $dbw ), $join->getAlias( $dbw ), $join->getSQL( $dbw ) );
			}
		}

		$builder->where( $this->getWhereSQL() );
		$builder->option( 'LIMIT', $this->limit );
		$builder->option( 'OFFSET', $this->offset );

		$config = MediaWikiServices::getInstance()->getMainConfig();
		$maxExecutionTime = $config->get( 'BucketMaxExecutionTime' );
		$builder->setMaxExecutionTime( $maxExecutionTime );

		$orderByField = $this->orderByField;
		$orderByDirection = $this->orderByDirection;
		if ( $orderByField && $orderByDirection ) {
			$builder->orderBy( $orderByField->getSafe( $dbw ), $orderByDirection );
		}

		return $builder;
	}

	private function parseWhere( array|string $condition ): QueryNode {
		if ( self::isOrAnd( $condition ) ) {
			$children = [];
			foreach ( $condition['operands'] as $key => $operand ) {
				$children[] = $this->parseWhere( $operand );
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
			$value = new Value( $condition[2] );
			return new ComparisonConditionNode( $selector, $op, $value );
		}
		if ( is_string( $condition ) && self::isCategory( $condition ) || ( is_array( $condition ) && self::isCategory( $condition[0] ) ) ) {
			$selector = new CategorySelector( $condition, $this );
			$this->addCategoryJoin( $condition );
			$op = new Operator( '!=' );
			$value = new Value( '&&NULL&&' );
			return new ComparisonConditionNode( $selector, $op, $value );
		}
		throw new QueryException( wfMessage( 'bucket-query-where-confused', json_encode( $condition ) ) );
	}
}

abstract class Join {
	abstract function getSQL( IDatabase $dbw ): array;

	abstract function getAlias(): string;
}

class BucketJoin extends Join {
	private BucketSchema $joinedTable;
	private FieldSelector $selector1;
	private FieldSelector $selector2;

	function __construct( BucketSchema $joinedTable, string $field1, string $field2, BucketQuery $query ) {
		$this->joinedTable = $joinedTable;
		$selector1 = new FieldSelector( $field1, $query );
		$selector2 = new FieldSelector( $field2, $query );
		// Cannot join two repeated fields
		if ( $selector1->getFieldSchema()->getRepeated() && $selector2->getFieldSchema()->getRepeated() ) {
			throw new QueryException( wfMessage( 'bucket-invalid-join-two-repeated', $selector1->getFieldSchema()->getFieldName(), $selector2->getFieldSchema()->getFieldName() ) );
		}
		// Cannot join with yourself
		if ( $selector1->getBucketSchema() == $selector2->getBucketSchema() ) {
			throw new QueryException( wfMessage( 'bucket-query-invalid-join' ) );
		}
		// One of the joined fields needs to be in the joined table
		if ( $selector1->getBucketSchema() != $joinedTable && $selector2->getBucketSchema() != $joinedTable ) {
			throw new QueryException( wfMessage( 'bucket-query-invalid-join' ) );
		}

		$this->selector1 = $selector1;
		$this->selector2 = $selector2;
	}

	function getAlias(): string {
		return $this->joinedTable->getTableName();
	}

	function getSQL( IDatabase $dbw ): array {
		$selector1 = $this->selector1;
		$selector1Safe = $selector1->getSafe( $dbw );
		$selector2 = $this->selector2;
		$selector2Safe = $selector2->getSafe( $dbw );
		if ( $selector1->getFieldSchema()->getRepeated() ) {
			return [ "$selector2Safe MEMBER OF($selector1Safe)" ];
		} elseif ( $selector2->getFieldSchema()->getRepeated() ) {
			return [ "$selector1Safe MEMBER OF($selector2Safe)" ];
		} else {
			return [ "$selector1Safe = $selector2Safe" ];
		}
	}
}

class CategoryJoin extends Join {
	private CategorySelector $categorySelector;
	private string $categoryName;
	private BucketQuery $query;

	function __construct( string $categoryName, BucketQuery $query ) {
		if ( !BucketQuery::isCategory( $categoryName ) ) {
			throw new QueryException( wfMessage( 'bucket-query-expected-category', $categoryName ) );
		}
		$this->categorySelector = new CategorySelector( $categoryName, $query );
		$this->categoryName = $categoryName;
		$this->query = $query;
	}

	function getAlias(): string {
		return $this->query->getCategoryAlias( $this->categoryName );
	}

	function getSQL( IDatabase $dbw ): array {
		$bucketTableName = $this->query->getPrimaryBucket()->getSafe( $dbw );
		$categoryNameNoPrefix = Title::newFromText( $this->categoryName, NS_CATEGORY )->getDBkey();
		return [
			"{$dbw->addIdentifierQuotes( $this->getAlias() )}.cl_from = {$bucketTableName}._page_id", // Must be all in one string to avoid the table name being treated as a string value.
			"{$this->categorySelector->getSafe($dbw)}" => $categoryNameNoPrefix
		];
	}
}

/**
 * @property QueryNode[] $children
 */
abstract class QueryNode {
	protected array $children;

	/**
	 * @return QueryNode[]
	 */
	function getChildren(): array {
		return $this->children;
	}

	abstract function getWhereSQL(): string;
}

class NotNode extends QueryNode {
	function __construct( QueryNode $child ) {
		$this->children = [ $child ];
	}

	function getWhereSQL(): string {
		return "(NOT ({$this->children[0]->getWhereSQL()}))";
	}
}

class OrNode extends QueryNode {
	/**
	 * @param QueryNode[] $children
	 */
	function __construct( array $children ) {
		$this->children = $children;
	}

	function getWhereSQL(): string {
		$childSQLs = array_map( static function ( QueryNode $child ) {
			return $child->getWhereSQL();
		}, $this->children );
		return Bucket::getDB()->makeList( $childSQLs, IDatabase::LIST_OR );
	}
}

class AndNode extends QueryNode {
	/**
	 * @param QueryNode[] $children
	 */
	function __construct( array $children ) {
		$this->children = $children;
	}

	function getWhereSQL(): string {
		$childSQLs = array_map( static function ( QueryNode $child ) {
			return $child->getWhereSQL();
		}, $this->children );
		return Bucket::getDB()->makeList( $childSQLs, IDatabase::LIST_AND );
	}
}

class ComparisonConditionNode extends QueryNode {
	private Selector $selector;
	private Operator $operator;
	private Value $value;

	function __construct( Selector $selector, Operator $operator, Value $value ) {
		$this->children = [];
		$this->selector = $selector;
		$this->operator = $operator;
		$this->value = $value;
	}

	function getSelector(): Selector {
		return $this->selector;
	}

	function getWhereSQL(): string {
		$dbw = Bucket::getDB();
		$selector = $this->selector;
		$fieldName = $selector->getSafe( $dbw );
		$op = $this->operator->getOperator();
		$value = $this->value->getSafe( $dbw );

		if ( $selector instanceof CategorySelector ) {
			return "({$selector->getSafe($dbw)} IS NOT NULL)";
		} elseif ( $selector instanceof FieldSelector ) {
			if ( $this->value->getValue() == '&&NULL&&' ) {
				if ( $op == '!=' ) {
					return "($fieldName IS NOT NULL)";
				}
				return "($fieldName IS NULL)";
			} elseif ( $selector->getFieldSchema()->getRepeated() == true ) {
				if ( $op == '=' ) {
					return "$value MEMBER OF($fieldName)";
				}
				if ( $op == '!=' ) {
					return "NOT $value MEMBER OF($fieldName)";
				}
				throw new QueryException( wfMessage( 'bucket-query-where-repeated-unsupported', $op, $fieldName ) );
			} else {
				if ( in_array( $op, [ '>', '>=', '<', '<=' ] ) ) {
					return $dbw->buildComparison( $op, [ $fieldName => $this->value->getValue() ] ); // Intentionally get the unquoted value, buildComparison quotes it.
				} elseif ( $op == '=' ) {
					return $dbw->makeList( [ $fieldName => $this->value->getValue() ], IDatabase::LIST_AND ); // Intentionally get the unquoted value, makeList quotes it.
				} elseif ( $op == '!=' ) {
					return "($fieldName $op $value)";
				}
			}
		}
		throw new QueryException( wfMessage( 'bucket-query-where-confused', json_encode( $this ) ) );
	}
}

class JoinCondition {
}

abstract class Selector {
	abstract function getSafe( IDatabase $dbw ): string;

	abstract function getSelectSQL( IDatabase $dbw ): string;
}

class FieldSelector extends Selector {
	private BucketSchema $schema;
	private BucketSchemaField $schemaField;

	function __construct( string $fullSelector, BucketQuery $query ) {
		$parts = explode( '.', $fullSelector ); // Split on period
		if ( $fullSelector === '' || count( $parts ) > 2 ) {
			throw new QueryException( wfMessage( 'bucket-query-field-name-invalid', $fullSelector ) );
		}
		$fieldName = end( $parts );
		if ( count( $parts ) == 1 ) { // If we don't have a period, we are the primary bucket.
			$this->schema = $query->getPrimaryBucket();
		} else {
			$usedBuckets = $query->getUsedBuckets();
			if ( !isset( $usedBuckets[$parts[0]] ) ) {
				throw new QueryException( wfMessage( 'bucket-query-bucket-not-found', $parts[0] ) );
			}
			$this->schema = $usedBuckets[$parts[0]];
		}
		$fields = $this->schema->getFields();
		if ( !isset( $fields[$fieldName] ) ) {
			throw new QueryException( wfMessage( 'bucket-query-field-not-found-in-bucket', $fieldName, $this->schema->getName() ) );
		}
		$this->schemaField = $fields[$fieldName];
	}

	function getSafe( IDatabase $dbw ): string {
		return $dbw->addIdentifierQuotes( Bucket::getBucketTableName( $this->schema->getName() ) ) . '.' . $dbw->addIdentifierQuotes( $this->schemaField->getFieldName() );
	}

	function getSelectSQL( IDatabase $dbw ): string {
		return $this->getSafe( $dbw );
	}

	function getFieldSchema(): BucketSchemaField {
		return $this->schemaField;
	}

	function getBucketSchema(): BucketSchema {
		return $this->schema;
	}
}

class CategorySelector extends Selector {
	private string $categoryName;
	private BucketQuery $query;

	function __construct( string $categoryName, BucketQuery $query ) {
		if ( !BucketQuery::isCategory( $categoryName ) ) {
			throw new QueryException( wfMessage( 'bucket-query-expected-category', $categoryName ) );
		}
		$this->categoryName = $categoryName;
		$this->query = $query;
	}

	function getSafe( IDatabase $dbw ): string {
		return $dbw->addIdentifierQuotes( $this->query->getCategoryAlias( $this->categoryName ) ) . '.cl_to';
	}

	function getSelectSQL( IDatabase $dbw ): string {
		return "{$this->getSafe($dbw)} IS NOT NULL";
	}
}

class Operator {
	private const WHERE_OPS = [
		'='  => true,
		'!=' => true,
		'>=' => true,
		'<=' => true,
		'>'  => true,
		'<'  => true,
	];

	private string $op;

	function __construct( string $operator ) {
		if ( !isset( self::WHERE_OPS[$operator] ) ) {
			throw new QueryException( wfMessage( 'bucket-query-where-invalid-op', $operator ) );
		}
		$this->op = $operator;
	}

	function getOperator(): string {
		return $this->op;
	}
}

class Value {
	private $value;

	function __construct( $value ) {
		if ( !is_scalar( $value ) ) {
			throw new QueryException( wfMessage( 'bucket-query-non-scalar' ) );
		}
		$this->value = $value;
	}

	function getValue() {
		return $this->value;
	}

	function getSafe( IDatabase $dbw ) {
		return $dbw->addQuotes( $this->value );
	}
}
