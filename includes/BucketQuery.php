<?php

namespace MediaWiki\Extension\Bucket;

use InvalidArgumentException;
use MediaWiki\Extension\Bucket\Expression\NotExpression;
use MediaWiki\MediaWikiServices;
use MediaWiki\Title\Title;
use Wikimedia\Rdbms\IDatabase;
use Wikimedia\Rdbms\IExpression;
use Wikimedia\Rdbms\RawSQLExpression;
use Wikimedia\Rdbms\SelectQueryBuilder;
use Wikimedia\Rdbms\Subquery;

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
	private array $repeatedFields = [];
	private BucketSchema $primarySchema;
	private array $joins = [];
	private array $selects = [];
	private QueryNode $where;
	private int $limit = self::DEFAULT_LIMIT;
	private int $offset = 0;
	private array $orderByFields = [];
	private ?Selector $userOrderByField = null;
	private string $orderByDirection = 'ASC';
	private int $categoryCount = 0;

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
			$categoryAlias = 'category' . $this->categoryCount++;
			$this->categories[$category] = $categoryAlias;
			$this->joins[] = new CategoryJoin( $category, $this );
		}
	}

	public function addRepeatedJoin( BucketSchema $parent, string $field ) {
		$uniqueName = $parent->getName() . $field;
		if ( !isset( $this->repeatedFields[$uniqueName] ) ) {
			$this->repeatedFields[$uniqueName] = true;
			$this->joins[] = new RepeatedFieldJoin( $parent, $field, $this );
		}
	}

	public function getCategoryAlias( string $category ): string {
		return $this->categories[$category];
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
		if ( isset( $this->where ) ) {
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

		foreach ( $this->selects as $selector ) {
			$builder->field( $selector->getSelectSQL( $dbw ) );
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
		if ( isset( $this->userOrderByField ) ) {
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
			foreach ( $condition['operands'] as $operand ) {
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

abstract class Join {
	abstract public function getSQL( IDatabase $dbw ): array;

	abstract public function getJoinTable( IDatabase $dbw ): string|SelectQueryBuilder;

	abstract public function getAlias(): string;
}

class BucketJoin extends Join {
	private BucketSchema $joinedTable;
	private FieldSelector $selector1;
	private FieldSelector $selector2;

	public function __construct( BucketSchema $joinedTable, string $field1, string $field2, BucketQuery $query ) {
		$this->joinedTable = $joinedTable;
		$selector1 = new FieldSelector( $field1, $query );
		$selector2 = new FieldSelector( $field2, $query );
		// Cannot join on a repeated field
		if ( $selector1->getFieldSchema()->getRepeated() ) {
			throw new QueryException( wfMessage(
				'bucket-query-invalid-join-repeated', $selector1->getFieldSchema()->getFieldName() ) );
		}
		if ( $selector2->getFieldSchema()->getRepeated() ) {
			throw new QueryException( wfMessage(
				'bucket-query-invalid-join-repeated', $selector2->getFieldSchema()->getFieldName() ) );
		}
		// Cannot join with yourself
		if ( $selector1->getBucketSchema() === $selector2->getBucketSchema() ) {
			throw new QueryException( wfMessage( 'bucket-query-invalid-join' ) );
		}
		// One of the joined fields needs to be in the joined table
		if ( $selector1->getBucketSchema() !== $joinedTable && $selector2->getBucketSchema() !== $joinedTable ) {
			throw new QueryException( wfMessage( 'bucket-query-invalid-join' ) );
		}

		$this->selector1 = $selector1;
		$this->selector2 = $selector2;
	}

	public function getJoinTable( IDatabase $dbw ): string {
		return $this->joinedTable->getTableName();
	}

	public function getAlias(): string {
		return $this->joinedTable->getTableName();
	}

	public function getSQL( IDatabase $dbw ): array {
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

class RepeatedFieldJoin extends Join {
	private BucketSchema $parent;
	private string $fieldName;

	public function __construct( BucketSchema $parent, string $repeatedField, BucketQuery $query ) {
		$this->parent = $parent;
		$this->fieldName = $repeatedField;
	}

	public function getJoinTable( IDatabase $dbw ): SelectQueryBuilder {
		$tableName = $dbw->tableName( $this->getAlias() );
		$safeFieldName = $dbw->addIdentifierQuotes( $this->fieldName );
		return $dbw->newSelectQueryBuilder()
			->from( $this->getAlias() )
			->select( [
				'_page_id',
				'_index',
				$this->fieldName => "JSON_ARRAYAGG($tableName.$safeFieldName)" ] )
			->groupBy( [ '_page_id', '_index' ] );
	}

	public function getAlias(): string {
		// TODO do we need to do something special here?
		return BucketDatabase::getRepeatedFieldTableName( $this->parent->getName(), $this->fieldName );
	}

	public function getSQL( IDatabase $dbw ): array {
		$bucketTableName = $this->parent->getSafe( $dbw );
		return [
			// Must be all in one string to avoid the table name being treated as a string value.
			"{$dbw->addIdentifierQuotes( $this->getAlias() )}._page_id = $bucketTableName._page_id",
			"{$dbw->addIdentifierQuotes( $this->getAlias() )}._index = $bucketTableName._index"
		];
	}
}

class CategoryJoin extends Join {
	private CategorySelector $categorySelector;
	private string $categoryName;
	private BucketQuery $query;

	public function __construct( string $categoryName, BucketQuery $query ) {
		if ( !BucketQuery::isCategory( $categoryName ) ) {
			throw new QueryException( wfMessage( 'bucket-query-expected-category', $categoryName ) );
		}
		$this->categorySelector = new CategorySelector( $categoryName, $query );
		$this->categoryName = $categoryName;
		$this->query = $query;
	}

	public function getJoinTable( IDatabase $dbw ): string {
		return $dbw->tableName( 'categorylinks' );
	}

	public function getAlias(): string {
		return $this->query->getCategoryAlias( $this->categoryName );
	}

	public function getSQL( IDatabase $dbw ): array {
		$bucketTableName = $this->query->getPrimaryBucket()->getSafe( $dbw );
		$categoryNameNoPrefix = Title::newFromText( $this->categoryName, NS_CATEGORY )->getDBkey();
		return [
			// Must be all in one string to avoid the table name being treated as a string value.
			"{$dbw->addIdentifierQuotes( $this->getAlias() )}.cl_from = $bucketTableName._page_id",
			"{$this->categorySelector->getSafe($dbw)}" => $categoryNameNoPrefix
		];
	}
}

/**
 * @property QueryNode[] $children
 */
abstract class QueryNode {
	protected array $children;

	abstract public function getWhereSQL( IDatabase $dbw ): IExpression;
}

class NotNode extends QueryNode {
	private QueryNode $child;

	public function __construct( QueryNode $child ) {
		$this->child = $child;
	}

	public function getWhereSQL( IDatabase $dbw ): IExpression {
		return new NotExpression( $this->child->getWhereSQL( $dbw ) );
	}
}

class OrNode extends QueryNode {
	/**
	 * @param QueryNode[] $children
	 */
	public function __construct( array $children ) {
		$this->children = $children;
	}

	public function getWhereSQL( IDatabase $dbw ): IExpression {
		$childSQLs = array_map( static function ( QueryNode $child ) use ( $dbw ) {
			return $child->getWhereSQL( $dbw );
		}, $this->children );
		return $dbw->orExpr( $childSQLs );
	}
}

class AndNode extends QueryNode {
	/**
	 * @param QueryNode[] $children
	 */
	public function __construct( array $children ) {
		$this->children = $children;
	}

	public function getWhereSQL( IDatabase $dbw ): IExpression {
		$childSQLs = array_map( static function ( QueryNode $child ) use ( $dbw ) {
			return $child->getWhereSQL( $dbw );
		}, $this->children );
		return $dbw->andExpr( $childSQLs );
	}
}

class ComparisonConditionNode extends QueryNode {
	private Selector $selector;
	private Operator $operator;
	private Value $value;

	public function __construct( Selector $selector, Operator $operator, Value $value ) {
		$this->selector = $selector;
		$this->operator = $operator;
		$this->value = $value;
	}

	public function getWhereSQL( IDatabase $dbw ): IExpression {
		$dbw = BucketDatabase::getDB();
		$selector = $this->selector;
		$fieldName = $selector->getUnsafe();
		$op = $this->operator->getOperator();
		$value = $this->value->getValue();

		if (
			$selector instanceof FieldSelector
			&& $value !== null
			&& $selector->getFieldSchema()->getRepeated() === true
		) {
			$repeatedFieldTable = BucketDatabase::getRepeatedFieldTableName(
				$selector->getBucketSchema()->getName(),
				$selector->getFieldSchema()->getFieldName() );
			$subquery = $dbw->newSelectQueryBuilder()
				->from( $repeatedFieldTable )
				->select( [ '_page_id', '_index' ] )
				->where( $dbw->expr( $selector->getUnsafe(), $op, $value ) )
				->caller( __METHOD__ );
			$subquery = $subquery->getSQL();
			$tableNameSafe = $dbw->tableName( $selector->getBucketSchema()->getTableName() );
			$in = "($tableNameSafe._page_id, $tableNameSafe._index) IN ";
			// TODO maybe make a SubqueryExpression?
			return new RawSQLExpression( $in . new Subquery( $subquery ) );
		} else {
			return $dbw->expr( $selector->getUnsafe(), $op, $value );
		}
	}
}

abstract class Selector {
	abstract public function getSafe( IDatabase $dbw ): string;

	abstract public function getUnsafe(): string;

	abstract public function getSelectSQL( IDatabase $dbw ): string;
}

class FieldSelector extends Selector {
	private BucketSchema $schema;
	private BucketSchemaField $schemaField;

	public function __construct( string $fullSelector, BucketQuery $query ) {
		// Split on period
		$parts = explode( '.', $fullSelector );
		if ( $fullSelector === '' || count( $parts ) > 2 ) {
			throw new QueryException( wfMessage( 'bucket-query-field-name-invalid', $fullSelector ) );
		}
		$fieldName = end( $parts );
		// If we don't have a period, we are the primary bucket.
		if ( count( $parts ) === 1 ) {
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
			throw new QueryException(
				wfMessage( 'bucket-query-field-not-found-in-bucket', $fieldName, $this->schema->getName() ) );
		}
		$this->schemaField = $fields[$fieldName];
	}

	public function getSafe( IDatabase $dbw ): string {
		// if ( $this->getFieldSchema()->getRepeated() ) {
		// 	$dbName = BucketDatabase::getRepeatedFieldTableName(
		// 		$this->schema->getName(), $this->schemaField->getFieldName() );
		// 	return $dbw->addIdentifierQuotes( $dbName )
		// 		. '.' . $dbw->addIdentifierQuotes( $this->schemaField->getFieldName() );
		// } else {
			return $dbw->addIdentifierQuotes( BucketDatabase::getBucketTableName( $this->schema->getName() ) )
				. '.' . $dbw->addIdentifierQuotes( $this->schemaField->getFieldName() );
		// }
	}

	public function getUnsafe(): string {
		if ( $this->getFieldSchema()->getRepeated() ) {
			// TODO check what this is used before and if the JSON_ARRAYAGG will break it
			return BucketDatabase::getRepeatedFieldTableName(
				$this->schema->getName(), $this->schemaField->getFieldName() )
				. '.' . $this->schemaField->getFieldName();

		} else {
			return BucketDatabase::getBucketTableName( $this->schema->getName() )
				. '.' . $this->schemaField->getFieldName();
		}
	}

	public function getSelectSQL( IDatabase $dbw ): string {
		return $this->getSafe( $dbw );
	}

	public function getFieldSchema(): BucketSchemaField {
		return $this->schemaField;
	}

	public function getBucketSchema(): BucketSchema {
		return $this->schema;
	}
}

class CategorySelector extends Selector {
	private string $categoryName;
	private BucketQuery $query;

	public function __construct( string $categoryName, BucketQuery $query ) {
		if ( !BucketQuery::isCategory( $categoryName ) ) {
			throw new QueryException( wfMessage( 'bucket-query-expected-category', $categoryName ) );
		}
		$this->categoryName = $categoryName;
		$this->query = $query;
	}

	public function getSafe( IDatabase $dbw ): string {
		return $dbw->addIdentifierQuotes( $this->query->getCategoryAlias( $this->categoryName ) ) . '.cl_to';
	}

	public function getUnsafe(): string {
		return $this->query->getCategoryAlias( $this->categoryName ) . '.cl_to';
	}

	public function getSelectSQL( IDatabase $dbw ): string {
		return $dbw->expr( $this->getUnsafe(), '!=', null )->toSql( $dbw );
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

	public function __construct( string $operator ) {
		if ( !isset( self::WHERE_OPS[$operator] ) ) {
			throw new QueryException( wfMessage( 'bucket-query-where-invalid-op', $operator ) );
		}
		$this->op = $operator;
	}

	public function getOperator(): string {
		return $this->op;
	}
}

class Value {
	private mixed $value;

	/**
	 * @param mixed $value
	 */
	public function __construct( $value ) {
		if ( $value !== null && !is_scalar( $value ) ) {
			throw new QueryException( wfMessage( 'bucket-query-non-scalar' ) );
		}
		$this->value = $value;
	}

	/**
	 * @return mixed
	 */
	public function getValue() {
		return $this->value;
	}
}
