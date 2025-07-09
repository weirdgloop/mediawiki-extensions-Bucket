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
	private ?FieldSelector $orderByField = null;
	private ?string $orderByDirection = null;

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
				$this->selects[] = new CategorySelector( $select );
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
			$this->orderByField = new FieldSelector( $orderByField, $this );
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
			$this->joins[] = new CategoryJoin( $category );
		}
	}

	/**
	 * Recursively walk a QueryNode tree and add all Category conditions to the join list
	 */
	private function checkForCategories( QueryNode $node ) {
		foreach ( $node->getChildren() as $child ) {
			if ( $child instanceof ComparisonConditionNode ) {
				$selector = $child->getSelector();
				if ( $selector instanceof CategorySelector ) {
					$this->addCategoryJoin( $selector->getInputString() );
				}
			}
			self::checkForCategories( $child );
		}
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

	public function getSelectQueryBuilder(): SelectQueryBuilder {
		$dbw = Bucket::getDB();
		$SELECTS = [];
		$querySelectors = $this->selects;
		foreach ( $querySelectors as $selector ) {
			$SELECTS[] = $selector->getSelectSQL( $dbw );
		}

		$LEFT_JOINS = [];
		$queryJoins = $this->joins;
		foreach ( $queryJoins as $join ) {
			if ( $join instanceof CategoryJoin ) {
				$LEFT_JOINS[$join->getName()] = $join->getSQL( $this, $dbw );
			} elseif ( $join instanceof BucketJoin ) {
				$LEFT_JOINS[Bucket::getBucketTableName( $join->getName() )] = $join->getSQL( $dbw );
			}
		}

		$WHERES = $this->where->getWhereSQL();
		if ( $WHERES == '' ) {
			$WHERES = []; // Select query builder doesn't like an empty string or null.
		}

		$OPTIONS = [];
		$OPTIONS['LIMIT'] = $this->limit;
		$OPTIONS['OFFSET'] = $this->offset;

		$config = MediaWikiServices::getInstance()->getMainConfig();
		$maxExecutionTime = $config->get( 'BucketMaxExecutionTime' );

		$tmp = $dbw->newSelectQueryBuilder()
			->from( Bucket::getBucketTableName( $this->getPrimaryBucket()->getName() ) )
			->select( $SELECTS )
			->where( $WHERES )
			->options( $OPTIONS )
			->caller( __METHOD__ )
			->setMaxExecutionTime( $maxExecutionTime );
		foreach ( $LEFT_JOINS as $alias => $conds ) {
			$name = $alias;
			if ( self::isCategory( $alias ) ) {
				$name = 'categorylinks';
			}
			$tmp->leftJoin( $name, $alias, $conds );
		}
		$orderByField = $this->orderByField;
		$orderByDirection = $this->orderByDirection;
		if ( $orderByField && $orderByDirection ) {
			$tmp->orderBy( $orderByField->getInputString(), $orderByDirection );
		}
		return $tmp;
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
			$selector = new CategorySelector( $condition );
			$this->addCategoryJoin( $condition );
			$op = new Operator( '!=' );
			$value = new Value( '&&NULL&&' );
			return new ComparisonConditionNode( $selector, $op, $value );
		}
		throw new QueryException( wfMessage( 'bucket-query-where-confused', json_encode( $condition ) ) );
	}
}

abstract class Join {
	abstract function getName(): string;

	abstract function getSafe( IDatabase $dbw ): string;
}

class BucketJoin extends Join {
	private BucketSchema $joinedTable;
	private JoinCondition $joinCondition;

	function __construct( BucketSchema $joinedTable, string $field1, string $field2, BucketQuery $query ) {
		$this->joinedTable = $joinedTable;
		$this->joinCondition = new JoinCondition( $field1, $field2, $query );
	}

	function getName(): string {
		return $this->joinedTable->getName();
	}

	function getSafe( IDatabase $dbw ): string {
		return $this->joinedTable->getSafe( $dbw );
	}

	function getSQL( IDatabase $dbw ): array {
		$condition = $this->joinCondition;
		$selector1 = $condition->getSelector1();
		$selector1Table = $selector1->getSafe( $dbw );
		$selector2 = $condition->getSelector2();
		$selector2Table = $selector2->getSafe( $dbw );
		if ( $selector1->getFieldSchema()->getRepeated() ) {
			return [ "$selector2Table MEMBER OF($selector1Table)" ];
		} elseif ( $selector2->getFieldSchema()->getRepeated() ) {
			return [ "$selector1Table MEMBER OF($selector2Table)" ];
		} else {
			return [ "$selector1Table = $selector2Table" ];
		}
	}
}

class CategoryJoin extends Join {
	private CategoryName $category;

	function __construct( string $category ) {
		$this->category = new CategoryName( $category );
	}

	function getName(): string {
		return $this->category->getName();
	}

	function getSafe( IDatabase $dbw ): string {
		return $this->category->getSafe( $dbw );
	}

	function getSQL( BucketQuery $query, IDatabase $dbw ): array {
		$bucketTableName = $query->getPrimaryBucket()->getSafe( $dbw );
		$categoryNameNoPrefix = Title::newFromText( $this->getName(), NS_CATEGORY )->getDBkey();
		return [
			"{$this->getSafe($dbw)}.cl_from = {$bucketTableName}._page_id", // Must be all in one string to avoid the table name being treated as a string value.
			"{$this->getSafe($dbw)}.cl_to" => $categoryNameNoPrefix
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
			return "({$selector->getSafe($dbw)}.cl_to IS NOT NULL)";
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
	private FieldSelector $selector1;
	private FieldSelector $selector2;

	function __construct( string $selector1, string $selector2, $query ) {
		$selector1 = new FieldSelector( $selector1, $query );
		$selector2 = new FieldSelector( $selector2, $query );

		if ( $selector1->getFieldSchema()->getRepeated() && $selector2->getFieldSchema()->getRepeated() ) {
			throw new QueryException( wfMessage( 'bucket-invalid-join-two-repeated', $selector1->getInputString(), $selector2->getInputString() ) );
		}
		$this->selector1 = $selector1;
		$this->selector2 = $selector2;
	}

	function getSelector1(): FieldSelector {
		return $this->selector1;
	}

	function getSelector2(): FieldSelector {
		return $this->selector2;
	}
}

abstract class Selector {
	private string $inputString; // Used to output as the same string that was input

	abstract function getSafe( IDatabase $dbw ): string;

	abstract function getSelectSQL( IDatabase $dbw ): string;

	function getInputString(): string {
		return $this->inputString;
	}

	protected function __construct( string $inputString ) {
		$this->inputString = $inputString;
	}
}
class FieldSelector extends Selector {
	private BucketSchema $schema;
	private BucketSchemaField $schemaField;

	function __construct( string $fullSelector, BucketQuery $query ) {
		parent::__construct( $fullSelector );
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
}

class CategorySelector extends Selector {
	private CategoryName $categoryName;

	function __construct( string $categoryName ) {
		parent::__construct( $categoryName );
		$this->categoryName = new CategoryName( $categoryName );
	}

	function getSafe( IDatabase $dbw ): string {
		return $this->categoryName->getSafe( $dbw );
	}

	function getSelectSQL( IDatabase $dbw ): string {
		return "{$this->getSafe($dbw)}.cl_to IS NOT NULL";
	}
}

abstract class Name {
	abstract function getName(): string;

	abstract function getSafe( IDatabase $dbw ): string;
}

class CategoryName extends Name {
	private string $categoryName;

	function __construct( string $name ) {
		if ( !BucketQuery::isCategory( $name ) ) {
			throw new QueryException( wfMessage( 'bucket-query-expected-category', $name ) );
		}
		$this->categoryName = $name;
	}

	function getName(): string {
		return $this->categoryName;
	}

	function getSafe( IDatabase $dbw ): string {
		return $dbw->addIdentifierQuotes( $this->categoryName );
	}
}

class BucketName extends Name {
	private BucketSchema $bucketSchema;

	function __construct( string $name, BucketQuery $query ) {
		$usedBuckets = $query->getUsedBuckets();
		if ( !isset( $usedBuckets[$name] ) ) {
			throw new QueryException( wfMessage( 'bucket-query-bucket-not-found', $name ) );
		}
		$this->bucketSchema = $usedBuckets[$name];
	}

	function getName(): string {
		return $this->bucketSchema->getName();
	}

	function getSafe( IDatabase $dbw ): string {
		return $this->bucketSchema->getSafe( $dbw );
	}
}

class FieldName extends Name {
	private string $fieldName;

	function __construct( string $name ) {
		$this->fieldName = Bucket::getValidFieldName( $name );
	}

	function getName(): string {
		return $this->fieldName;
	}

	function getSafe( IDatabase $dbw ): string {
		return $dbw->addIdentifierQuotes( $this->fieldName );
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
