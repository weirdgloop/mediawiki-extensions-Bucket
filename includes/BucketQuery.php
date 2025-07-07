<?php

namespace MediaWiki\Extension\Bucket;

use MediaWiki\Title\Title;
use Wikimedia\Rdbms\IDatabase;

class BucketQuery {
	public const MAX_LIMIT = 5000;
	public const DEFAULT_LIMIT = 500;
	// Caches schemas that are read from the database, so multiple queries on one page can reuse them.
	private static $schemaCache = [];

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

	public static function cast( $value, BucketSchemaField $fieldData ) {
		$type = $fieldData->getType();
		if ( $fieldData->getRepeated() ) {
			$ret = [];
			if ( $value == null ) {
				$value = '';
			}
			$jsonData = json_decode( $value, true );
			if ( !is_array( $jsonData ) ) { // If we are in a repeated field but only holding a scalar, make it an array anyway.
				$jsonData = [ $jsonData ];
			}
			$nonRepeatedData = new BucketSchemaField( $fieldData->getFieldName(), $fieldData->getType(), $fieldData->getIndexed(), false );
			foreach ( $jsonData as $subVal ) {
				$ret[] = self::cast( $subVal, $nonRepeatedData );
			}
			return $ret;
		} elseif ( $type === 'TEXT' || $type === 'PAGE' ) {
			return $value;
		} elseif ( $type === 'DOUBLE' ) {
			return floatval( $value );
		} elseif ( $type === 'INTEGER' ) {
			return intval( $value );
		} elseif ( $type === 'BOOLEAN' ) {
			return boolval( $value );
		}
	}

	private static function generateWhereSQL( QueryNode $node ): string {
		if ( $node instanceof AndNode || $node instanceof OrNode ) {
			$children = [];
			foreach ( $node->getChildren() as $child ) {
				$children[] = self::generateWhereSQL( $child );
			}
			if ( $node instanceof OrNode ) {
				return Bucket::getDB()->makeList( $children, IDatabase::LIST_OR );
			} else {
				return Bucket::getDB()->makeList( $children, IDatabase::LIST_AND );
			}
		} elseif ( $node instanceof NotNode ) {
			$child = self::generateWhereSQL( $node->getChild() );
			return "(NOT ($child))";
		} elseif ( $node instanceof ComparisonConditionNode ) {
			$dbw = Bucket::getDB();
			$selector = $node->getSelector();
			$fieldName = $selector->getQuotedSelectorText( $dbw );
			$op = $node->getOperator()->getOperator();
			$value = $node->getValue()->getQuotedValue( $dbw );

			if ( $selector instanceof CategorySelector ) {
				return "({$selector->getQuotedSelectorText($dbw)}.cl_to IS NOT NULL)";
			} elseif ( $selector instanceof FieldSelector ) {
				if ( $value == '&&NULL&&' ) {
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
					// > < >= <=
					//TODO this is very expensive
					// $columnData['repeated'] = false; // Set repeated to false to get the underlying type
					// $dbType = Bucket::getDbType( $columnNameData['fullName'], $columnData );
					// We have to reverse the direction of < > <= >= because SQL requires this condition to be $value $op $column
					//and user input is in order $column $op $value
					// $op = strtr( $op, [ '<' => '>', '>' => '<' ] );
					// return "($value $op ANY(SELECT json_col FROM JSON_TABLE($columnName, '$[*]' COLUMNS(json_col $dbType PATH '$')) AS json_tab))";
				} else {
					if ( in_array( $op, [ '>', '>=', '<', '<=' ] ) ) {
						return $dbw->buildComparison( $op, [ $fieldName => $node->getValue()->getValue() ] ); // Intentionally get the unquoted value, buildComparison quotes it.
					} elseif ( $op == '=' ) {
						return $dbw->makeList( [ $fieldName => $node->getValue()->getValue() ], IDatabase::LIST_AND ); // Intentionally get the unquoted value, makeList quotes it.
					} elseif ( $op == '!=' ) {
						return "($fieldName $op $value)";
					}
				}
			}
		}
		throw new QueryException( wfMessage( 'bucket-query-where-confused', json_encode( $node ) ) );
	}

	public static function runSelect( $data ) {
		self::parseSelect( $data ); // This populates the query instance

		// unset $data so that we cannot cross contaminate
		$data = null;

		$dbw = Bucket::getDB();

		$SELECTS = [];
		$querySelectors = self::$query->getSelectors();
		foreach ( $querySelectors as $selector ) {
			if ( $selector instanceof CategorySelector ) {
				$SELECTS[$selector->getInputString()] = "{$selector->getQuotedSelectorText($dbw)}.cl_to IS NOT NULL";
			} else {
				$SELECTS[$selector->getInputString()] = $selector->getQuotedSelectorText( $dbw );
			}
		}

		$LEFT_JOINS = [];
		$queryJoins = self::$query->getJoins();
		foreach ( $queryJoins as $join ) {
			if ( $join instanceof CategoryJoin ) {
				$bucketTableName = self::$query->getPrimaryBucket()->getQuotedTableName( $dbw );
				$categoryNameNoPrefix = Title::newFromText( $join->getName(), NS_CATEGORY )->getDBkey();
				$LEFT_JOINS[$join->getName()] = [
					"{$join->getQuotedIdentifier($dbw)}.cl_from = {$bucketTableName}._page_id", // Must be all in one string to avoid the table name being treated as a string value.
					"{$join->getQuotedIdentifier($dbw)}.cl_to" => $categoryNameNoPrefix
				];
			} elseif ( $join instanceof BucketJoin ) {
				$condition = $join->getJoinCondition();

				$selector1 = $condition->getSelector1();
				$selector1Table = $selector1->getQuotedSelectorText( $dbw );
				$selector2 = $condition->getSelector2();
				$selector2Table = $selector2->getQuotedSelectorText( $dbw );
				if ( $selector1->getFieldSchema()->getRepeated() ) {
					$LEFT_JOINS[Bucket::getBucketTableName( $join->getName() )] = [
						"$selector2Table MEMBER OF($selector1Table)"
					];
				} elseif ( $selector2->getFieldSchema()->getRepeated() ) {
					$LEFT_JOINS[Bucket::getBucketTableName( $join->getName() )] = [
						"$selector1Table MEMBER OF($selector2Table)"
					];
				} else {
					$LEFT_JOINS[Bucket::getBucketTableName( $join->getName() )] = [
						"$selector1Table = $selector2Table"
					];
				}
			}
		}

		$WHERES = self::generateWhereSQL( self::$query->getWhereNodes() );
		if ( $WHERES == '' ) {
			$WHERES = []; // Select query builder doesn't like an empty string or null.
		}

		$OPTIONS = [];
		$OPTIONS['LIMIT'] = self::$query->getLimit();
		$OPTIONS['OFFSET'] = self::$query->getOffset();

		$rows = [];
		$tmp = $dbw->newSelectQueryBuilder()
			->from( Bucket::getBucketTableName( self::$query->getPrimaryBucket()->getName() ) )
			->select( $SELECTS )
			->where( $WHERES )
			->options( $OPTIONS )
			->caller( __METHOD__ )
			->setMaxExecutionTime( 500 );
		foreach ( $LEFT_JOINS as $alias => $conds ) {
			$name = $alias;
			if ( self::isCategory( $alias ) ) {
				$name = 'categorylinks';
			}
			$tmp->leftJoin( $name, $alias, $conds );
		}
		$orderByField = self::$query->getOrderByField();
		$orderByDirection = self::$query->getOrderByDirection();
		if ( $orderByField && $orderByDirection ) {
			$tmp->orderBy( $orderByField->getInputString(), $orderByDirection );
		}
		$sql_string = '';
		if ( self::$query->getDebug() == true ) {
			$sql_string = $tmp->getSQL();
		}
		$res = $tmp->fetchResultSet();
		foreach ( $res as $row ) {
			$row = (array)$row;
			foreach ( $row as $fieldName => $value ) {
				if ( self::isCategory( $fieldName ) ) {
					$row[$fieldName] = boolval( $value );
				} else {
					$selector = new FieldSelector( $fieldName );
					$row[$selector->getInputString()] = self::cast( $value, $selector->getFieldSchema() );
				}
			}
			$rows[] = $row;
		}
		return [ $rows, $sql_string ];
	}

	/**
	 * This function takes in the array object produced by Lua and productes a Query object
	 */
	static Query $query;

	private static function parseSelect( $data ) {
		// Start with populating the schema cache for all used buckets.
		$usedBuckets = [];

		$usedBuckets[] = $data['bucketName'];
		foreach ( $data['joins'] as $join ) {
			$usedBuckets[] = $join['bucketName'];
		}

		// Populate the schema cache
		$neededSchemas = $usedBuckets;
		foreach ( $neededSchemas as $name ) {
			if ( isset( self::$schemaCache[$name] ) ) {
				unset( $neededSchemas[$name] );
			}
		}
		$dbw = Bucket::getDB();
		if ( !empty( $neededSchemas ) ) {
			$res = $dbw->newSelectQueryBuilder()
				->from( 'bucket_schemas' )
				->select( [ 'bucket_name', 'schema_json' ] )
				->lockInShareMode()
				->where( [ 'bucket_name' => $neededSchemas ] )
				->caller( __METHOD__ )
				->fetchResultSet();
			foreach ( $res as $row ) {
				self::$schemaCache[$row->bucket_name] = json_decode( $row->schema_json, true );
			}
		}
		foreach ( $usedBuckets as $bucketName ) {
			if ( !array_key_exists( $bucketName, self::$schemaCache ) || !self::$schemaCache[$bucketName] ) {
				throw new QueryException( wfMessage( 'bucket-no-exist', $bucketName ) );
			}
		}

		// Create the query object, using previously retrieved schemas
		self::$query = new Query( $data['bucketName'], self::$schemaCache[$data['bucketName']] );

		foreach ( $data['joins'] as $join ) {
			if ( !is_array( $join['cond'] ) || !count( $join['cond'] ) == 2 ) {
				throw new QueryException( wfMessage( 'bucket-query-invalid-join', json_encode( $join ) ) );
			}
			self::$query->addBucketJoin( $join['bucketName'], self::$schemaCache[$join['bucketName']], $join['cond'][0], $join['cond'][1] );
		}

		// Parse selects
		if ( empty( $data['selects'] ) ) {
			throw new QueryException( wfMessage( 'bucket-query-select-empty' ) );
		}
		foreach ( $data['selects'] as $selectField ) {
			self::$query->addSelect( $selectField );
		}

		// Parse wheres
		self::$query->addWhere( self::parseWhere( $data['wheres'] ) );

		// Parse LIMIT
		if ( isset( $data['limit'] ) && is_int( $data['limit'] ) && $data['limit'] >= 0 ) {
			self::$query->setLimit( $data['limit'] );
		}

		if ( isset( $data['offset'] ) && is_int( $data['offset'] ) && $data['offset'] >= 0 ) {
			self::$query->setOffset( $data['offset'] );
		}

		if ( isset( $data['orderBy'] ) ) {
			self::$query->setOrderBy( $data['orderBy']['fieldName'], $data['orderBy']['direction'] );
		}

		if ( isset( $data['debug'] ) ) {
			self::$query->setDebug();
		}
	}

	private static function parseWhere( array|string $condition ): QueryNode {
		if ( self::isOrAnd( $condition ) ) {
			$children = [];
			foreach ( $condition['operands'] as $key => $operand ) {
				$children[] = self::parseWhere( $operand );
			}
			if ( $condition['op'] === 'OR' ) {
				return new OrNode( $children );
			} else {
				return new AndNode( $children );
			}
		}
		if ( self::isNot( $condition ) ) {
			return new NotNode( self::parseWhere( $condition['operand'] ) );
		}
		if ( is_array( $condition ) && isset( $condition[0] ) && isset( $condition[1] ) && isset( $condition[2] ) ) {
			$selector = new FieldSelector( $condition[0] );
			$op = new Operator( $condition[1] );
			$value = new Value( $condition[2] );
			return new ComparisonConditionNode( $selector, $op, $value );
		}
		if ( is_string( $condition ) && self::isCategory( $condition ) || ( is_array( $condition ) && self::isCategory( $condition[0] ) ) ) {
			$selector = new CategorySelector( $condition );
			$op = new Operator( '!=' );
			$value = new Value( '&&NULL&&' );
			return new ComparisonConditionNode( $selector, $op, $value );
		}
		throw new QueryException( wfMessage( 'bucket-query-where-confused', json_encode( $condition ) ) );
	}
}

/**
 * @property BucketSchema[] $schemas - The schema objects, populated from self::$schemaCache
 * @property Join[] $joins
 * @property Selector[] $selects
 */
class Query {
	private array $schemas = [];
	private array $categories = [];
	private BucketSchema $primarySchema;
	private array $joins = [];
	private array $selects = [];
	private QueryNode $where;
	private int $limit = BucketQuery::DEFAULT_LIMIT;
	private int $offset = 0;
	private ?FieldSelector $orderByField = null;
	private ?string $orderByDirection = null;
	private bool $debug = false;

	function __construct( string $primaryTable, array $schema ) {
		$primarySchema = new BucketSchema( $primaryTable, $schema );
		$this->schemas[$primarySchema->getName()] = $primarySchema;
		$this->primarySchema = $primarySchema;
	}

	function addBucketJoin( string $joinTable, array $schema, string $field1, string $field2 ) {
		$joinTableSchema = new BucketSchema( $joinTable, $schema );
		$this->schemas[$joinTableSchema->getName()] = $joinTableSchema;
		$join = new BucketJoin( $joinTableSchema, $field1, $field2 );
		$joinedTableName = $joinTableSchema->getName();
		if ( isset( $joins[$joinedTableName] ) ) {
			throw new QueryException( wfMessage( 'bucket-select-duplicate-join', $joinedTableName ) );
		}
		$this->joins[] = $join;
	}

	private function addCategoryJoin( string $category ) {
		if ( !isset( $this->categories[$category] ) ) {
			$this->joins[] = new CategoryJoin( $category );
		}
	}

	/**
	 * @return Join[]
	 */
	function getJoins(): array {
		return $this->joins;
	}

	function addSelect( string $select ) {
		if ( BucketQuery::isCategory( $select ) ) {
			$this->selects[] = new CategorySelector( $select );
			$this->addCategoryJoin( $select );
		} else {
			$this->selects[] = new FieldSelector( $select );
		}
	}

	/**
	 * @return Selector[]
	 */
	function getSelectors(): array {
		return $this->selects;
	}

	function addWhere( QueryNode $where ) {
		$this->checkForCategories( $where );
		$this->where = $where;
	}

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

	function getWhereNodes(): QueryNode {
		return $this->where;
	}

	function setLimit( int $limit ) {
		if ( $limit > 0 ) {
			$this->limit = min( $limit, BucketQuery::MAX_LIMIT );
		}
	}

	function getLimit(): int {
		return $this->limit;
	}

	function setOffset( int $offset ) {
		if ( $offset > 0 ) {
			$this->offset = $offset;
		}
	}

	function getOffset(): int {
		return $this->offset;
	}

	function setOrderBy( string $orderByField, string $direction ) {
		$isSelected = false;
		$this->orderByField = new FieldSelector( $orderByField );
		foreach ( BucketQuery::$query->getSelectors() as $select ) {
			if ( $select->getSelectorText() == $this->orderByField->getSelectorText() ) {
				$isSelected = true;
				break;
			}
		}
		if ( !$isSelected ) {
			throw new QueryException( wfMessage( 'bucket-query-order-by-must-select', $orderByField ) );
		}
		if ( $direction != 'ASC' && $direction != 'DESC' ) {
			throw new QueryException( wfMessage( 'bucket-query-order-by-direction', $direction ) );
		}
		$this->orderByDirection = $direction;
	}

	function getOrderByField(): ?FieldSelector {
		return $this->orderByField;
	}

	function getOrderByDirection(): ?string {
		return $this->orderByDirection;
	}

	function getPrimaryBucket(): BucketSchema {
		return $this->primarySchema;
	}

	/**
	 * @return BucketSchema[]
	 */
	function getUsedBuckets(): array {
		return $this->schemas;
	}

	function setDebug() {
		$this->debug = true;
	}

	function getDebug() {
		return $this->debug;
	}
}

abstract class Join {
	abstract function getName(): string;

	abstract function getQuotedIdentifier( IDatabase $dbw ): string;
}

class BucketJoin extends Join {
	private BucketSchema $joinedTable;
	private JoinCondition $joinCondition;

	function __construct( BucketSchema $joinedTable, string $field1, string $field2 ) {
		$this->joinedTable = $joinedTable;
		$this->joinCondition = new JoinCondition( $field1, $field2 );
	}

	function getJoinCondition(): JoinCondition {
		return $this->joinCondition;
	}

	function getName(): string {
		return $this->joinedTable->getName();
	}

	function getQuotedIdentifier( IDatabase $dbw ): string {
		return $this->joinedTable->getQuotedTableName( $dbw );
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

	function getQuotedIdentifier( IDatabase $dbw ): string {
		return $this->category->getQuotedIdentifier( $dbw );
	}
}

/**
 * @property QueryNode[] $children
 */
class QueryNode {
	protected array $children;

	/**
	 * @return QueryNode[]
	 */
	function getChildren(): array {
		return $this->children;
	}
}

class NotNode extends QueryNode {
	function __construct( QueryNode $child ) {
		$this->children = [ $child ];
	}

	function getChild(): QueryNode {
		return $this->children[0];
	}
}

class OrNode extends QueryNode {
	/**
	 * @param QueryNode[] $children
	 */
	function __construct( array $children ) {
		$this->children = $children;
	}
}

class AndNode extends QueryNode {
	/**
	 * @param QueryNode[] $children
	 */
	function __construct( array $children ) {
		$this->children = $children;
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

	function getOperator(): Operator {
		return $this->operator;
	}

	function getValue(): Value {
		return $this->value;
	}
}

class JoinCondition {
	private FieldSelector $selector1;
	private FieldSelector $selector2;

	function __construct( string $selector1, string $selector2 ) {
		$selector1 = new FieldSelector( $selector1 );
		$selector2 = new FieldSelector( $selector2 );

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

	abstract function getSelectorText(): string;

	abstract function getQuotedSelectorText( IDatabase $dbw ): string;

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

	function __construct( string $fullSelector ) {
		parent::__construct( $fullSelector );
		$parts = explode( '.', $fullSelector ); // Split on period
		if ( $fullSelector === '' || count( $parts ) > 2 ) {
			throw new QueryException( wfMessage( 'bucket-query-field-name-invalid', $fullSelector ) );
		}
		$fieldName = end( $parts );
		if ( count( $parts ) == 1 ) { // If we don't have a period, we are the primary bucket.
			$this->schema = BucketQuery::$query->getPrimaryBucket();
		} else {
			$usedBuckets = BucketQuery::$query->getUsedBuckets();
			if ( !isset( $usedBuckets[$parts[0]] ) ) {
				throw new QueryException( wfMessage( 'bucket-query-bucket-not-found', $parts[0] ) );
			}
			$this->schema = $usedBuckets[$parts[0]];
		}
		// TODO ambiguous checks - I don't think anything can be ambiguous now that everything except the primary bucket requires a bucket name
		$fields = $this->schema->getFields();
		if ( !isset( $fields[$fieldName] ) ) {
			debug_print_backtrace();
			throw new QueryException( wfMessage( 'bucket-query-field-not-found-in-bucket', $fieldName, $this->schema->getName() ) );
		}
		$this->schemaField = $fields[$fieldName];
	}

	function getSelectorText(): string {
		return $this->schema->getName() . '.' . $this->schemaField->getFieldName();
	}

	function getQuotedSelectorText( IDatabase $dbw ): string {
		return $dbw->addIdentifierQuotes( Bucket::getBucketTableName( $this->schema->getName() ) ) . '.' . $dbw->addIdentifierQuotes( $this->schemaField->getFieldName() );
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

	function getSelectorText(): string {
		return $this->categoryName->getName();
	}

	function getQuotedSelectorText( IDatabase $dbw ): string {
		return $this->categoryName->getQuotedIdentifier( $dbw );
	}
}

abstract class Name {
	abstract function getName(): string;

	abstract function getQuotedIdentifier( IDatabase $dbw ): string;
}
class CategoryName extends Name {
	private string $categoryName;

	function __construct( string $name ) {
		// TODO idk what to do here if this fails?
		assert( substr( strtolower( trim( $name ) ), 0, 9 ) == 'category:' );
		$this->categoryName = $name;
	}

	function getName(): string {
		return $this->categoryName;
	}

	function getQuotedIdentifier( IDatabase $dbw ): string {
		return $dbw->addIdentifierQuotes( $this->categoryName );
	}
}

class BucketName extends Name {
	private BucketSchema $bucketSchema;

	function __construct( string $name ) {
		$usedBuckets = BucketQuery::$query->getUsedBuckets();
		if ( !isset( $usedBuckets[$name] ) ) {
			throw new QueryException( wfMessage( 'bucket-query-bucket-not-found', $name ) );
		}
		$this->bucketSchema = $usedBuckets[$name];
	}

	function getName(): string {
		return $this->bucketSchema->getName();
	}

	function getQuotedIdentifier( IDatabase $dbw ): string {
		return $this->bucketSchema->getQuotedTableName( $dbw );
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

	function getQuotedIdentifier( IDatabase $dbw ): string {
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

	private function castValue( $value ) {
		if ( !is_scalar( $value ) ) {
			throw new QueryException( wfMessage( 'bucket-query-non-scalar' ) );
		}
		if ( is_int( $value ) ) {
			return intval( $value );
		}
		if ( is_float( $value ) ) { // Float and double
			return floatval( $value );
		}
		if ( is_bool( $value ) ) {
			// MySQL doesn't have boolean, 0 = FALSE and 1 = TRUE
			if ( $value ) {
				return 1;
			} else {
				return 0;
			}
		}
		if ( is_string( $value ) ) {
			return strval( $value );
		}
		throw new QueryException( wfMessage( 'bucket-query-cast-fail', $value ) );
	}

	function __construct( $value ) {
		$this->value = self::castValue( $value );
	}

	function getValue() {
		return $this->value;
	}

	function getQuotedValue( IDatabase $dbw ) {
		return $dbw->addQuotes( $this->value );
	}
}

/**
 * @property BucketSchemaField[] $fields
 */
class BucketSchema {
	private string $bucketName;
	private array $fields = [];

	function __construct( string $bucketName, array $schema ) {
		if ( $bucketName == '' ) {
			throw new QueryException( wfMessage( 'bucket-empty-bucket-name' ) );
		}
		$this->bucketName = $bucketName;

		foreach ( $schema as $name => $val ) {
			// Skip the _time field, its not a real field
			if ( $name == '_time' ) {
				continue;
			}
			$this->fields[$name] = new BucketSchemaField(
				$name,
				$val['type'],
				$val['index'],
				$val['repeated']
			);
		}
	}

	/**
	 * @return BucketSchemaField[]
	 */
	function getFields(): array {
		return $this->fields;
	}

	function getName(): string {
		return $this->bucketName;
	}

	function getTableName(): string {
		return Bucket::getBucketTableName( $this->bucketName );
	}

	function getQuotedTableName( IDatabase $dbw ): string {
		return $dbw->addIdentifierQuotes( $this->getTableName() );
	}
}

class BucketSchemaField {
	private string $fieldName;
	private string $type;
	private bool $indexed;
	private bool $repeated;

	function __construct( string $fieldName, string $type, bool $indexed, bool $repeated ) {
		$this->fieldName = $fieldName;
		$this->type = $type;
		$this->indexed = $indexed;
		$this->repeated = $repeated;
	}

	function getFieldName(): string {
		return $this->fieldName;
	}

	function getType(): string {
		return $this->type;
	}

	function getIndexed(): bool {
		return $this->indexed;
	}

	function getRepeated(): bool {
		return $this->repeated;
	}
}
