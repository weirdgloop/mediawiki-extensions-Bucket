<?php

namespace MediaWiki\Extension\Bucket;

use JsonSerializable;
use LogicException;
use MediaWiki\Logger\LoggerFactory;
use MediaWiki\MediaWikiServices;
use MediaWiki\Message\Message;
use Wikimedia\Rdbms\IDatabase;
use Wikimedia\Rdbms\IExpression;

class Bucket {
	public const EXTENSION_DATA_KEY = 'bucket:puts';
	public const EXTENSION_BUCKET_NAMES_KEY = 'bucket:puts_bucket_names';
	public const ISSUES_BUCKET = 'bucket_issues';
	public const REPEATED_CHARACTER_LIMIT = 512;
	public const REPEATED_CHARACTER_TOTAL_LIMIT = 5254;
	public const TEXT_BYTE_LIMIT = 65535;

	private static function isValidName( string $name ): bool {
		if (
			// Disallow numeric names as the MW RDBMS treats numeric tables names as ints in some circumstances.
			is_numeric( $name ) === false
			&& !str_starts_with( $name, '_' )
			&& strpos( $name, '__' ) === false
			&& preg_match( '/^[a-zA-Z0-9_]+$/D', $name )
		) {
			return true;
		}
		return false;
	}

	public static function getValidFieldName( string $bucketName, ?string $fieldName ): string {
		if ( $fieldName !== null
			&& self::isValidName( $fieldName )
		) {
			$cleanName = strtolower( trim( $fieldName ) );
			$dbw = BucketDatabase::getDB();
			$fullName = $dbw->tableName( BucketDatabase::getSubTableName( $bucketName, $cleanName ), 'raw' );
			// MySQL has a maximum of 64 characters for a table name
			if ( strlen( $fullName ) <= 64 ) {
				return $cleanName;
			} else {
				throw new SchemaException(
					wfMessage( 'bucket-long-field-name-error' )
						->params( $fieldName )
						->numParams( [
							strlen( $cleanName ),
							64 - strlen( $dbw->tableName( BucketDatabase::getSubTableName( $bucketName, '' ), 'raw' ) )
						] ) );
			}
		}
		throw new SchemaException( wfMessage( 'bucket-schema-invalid-field-name', $fieldName ) );
	}

	public static function getValidBucketName( string $bucketName ): string {
		if ( ucfirst( $bucketName ) !== ucfirst( strtolower( $bucketName ) ) ) {
			throw new SchemaException( wfMessage( 'bucket-capital-name-error' ) );
		}
		if ( self::isValidName( $bucketName ) ) {
			$cleanName = strtolower( trim( $bucketName ) );
			$dbw = BucketDatabase::getDB();
			$fullName = $dbw->tableName( BucketDatabase::getSubTableName( $bucketName, '' ), 'raw' );
			if ( strlen( $fullName ) <= 55 ) {
				return $cleanName;
			} else {
				throw new SchemaException(
					wfMessage( 'bucket-long-bucket-name-error' )
						->params( $bucketName )
						->numParams( [
							strlen( $cleanName ),
							55 - strlen( $dbw->tableName( BucketDatabase::getBucketTableName( '' ), 'raw' ) )
						] ) );
			}
		}
		throw new SchemaException( wfMessage( 'bucket-invalid-name-warning', $bucketName ) );
	}

	public static function writePuts( int $pageId, string $titleText, array $puts ) {
		( new BucketWriter() )->writePuts( $pageId, $titleText, $puts );
	}

	/**
	 * @param array $userInput
	 * @return array
	 */
	public static function runSelect( array $userInput ): array {
		$query = new BucketQuery( $userInput );
		$fieldNames = $query->getFields();
		$selectQueryBuilder = $query->getSelectQueryBuilder();

		$sql_string = '';

		if ( isset( $userInput['debug'] ) ) {
			$sql_string = $selectQueryBuilder->getSQL();
		}
		$res = $selectQueryBuilder->fetchResultSet();
		$result = [];

		// phpcs:ignore Generic.CodeAnalysis.AssignmentInCondition.FoundInWhileCondition
		while ( $dataRow = $res->fetchRow() ) {
			$resultRow = [];
			foreach ( $dataRow as $key => $value ) {
				if ( is_numeric( $key ) === false ) {
					continue;
				}
				$field = $fieldNames[$key];
				if ( $field instanceof FieldSelector ) {
					$resultRow[$userInput['selects'][$key]] = $field->getFieldSchema()->castValueForLua( $value );
				} else {
					$resultRow[$userInput['selects'][$key]] = boolval( $value );
				}
			}
			$result[] = $resultRow;
		}

		$generalized_sql = null;
		$conds = $selectQueryBuilder->getQueryInfo()['conds'];
		if ( is_array( $conds ) && count( $conds ) > 0 && $conds[0] instanceof IExpression ) {
			$generalized_sql = $conds[0]->toGeneralizedSql();
		}

		$page = MediaWikiServices::getInstance()->getParser()->getPage();

		LoggerFactory::getInstance( 'bucket' )->debug( 'bucket query',
			[
				'bucket_count' => count( $result ),
				'bucket_sql' => $selectQueryBuilder->getSQL(),
				'bucket_generalized_sql' => $generalized_sql,
				'bucket_page' => $page ? $page->__toString() : null
			] );

		return [ $result, $sql_string ];
	}
}

enum BucketValueType: string {
	case Text = 'TEXT';
	case Page = 'PAGE';
	case Double = 'DOUBLE';
	case Integer = 'INTEGER';
	case Boolean = 'BOOLEAN';
}

enum DatabaseValueType: string {
	case Text = 'TEXT';
	case Double = 'DOUBLE';
	case Integer = 'INTEGER';
	case Boolean = 'BOOLEAN';
	case Json = 'JSON';
}

/**
 * @property BucketSchemaField[] $fields
 */
class BucketSchema implements JsonSerializable {
	private string $bucketName;
	private array $fields = [];
	private int $timestamp;

	public function __construct( string $bucketName, array $schema ) {
		$this->timestamp = time();
		if ( $bucketName === '' ) {
			throw new QueryException( wfMessage( 'bucket-empty-bucket-name' ) );
		}
		$this->bucketName = $bucketName;

		foreach ( $schema as $name => $val ) {
			if ( $val instanceof BucketSchemaField ) {
				$this->fields[$val->getFieldName()] = $val;
			} else {
				// Skip the _time field, its not a real field
				if ( $name === '_time' ) {
					$this->timestamp = $val;
					continue;
				}
				$this->fields[$name] = new BucketSchemaField(
					$name,
					BucketValueType::from( $val['type'] ),
					$val['index'],
					$val['repeated']
				);
			}
		}
	}

	/**
	 * @return BucketSchemaField[]
	 */
	public function getFields(): array {
		return $this->fields;
	}

	public function getField( string $fieldName ): BucketSchemaField {
		return $this->fields[$fieldName];
	}

	public function getName(): string {
		return $this->bucketName;
	}

	public function getTableName(): string {
		return BucketDatabase::getBucketTableName( $this->bucketName );
	}

	public function getSafe( IDatabase $dbw ): string {
		return $dbw->addIdentifierQuotes( $this->getTableName() );
	}

	public function jsonSerialize(): array {
		$fields = $this->getFields();
		$fields['_time'] = $this->timestamp;
		return $fields;
	}
}

class BucketSchemaField implements JsonSerializable {
	private string $fieldName;
	private BucketValueType $type;
	private bool $indexed;
	private bool $repeated;

	public function __construct( string $fieldName, BucketValueType $type, bool $indexed, bool $repeated ) {
		$this->fieldName = $fieldName;
		$this->type = $type;
		$this->indexed = $indexed;
		$this->repeated = $repeated;
	}

	public function getFieldName(): string {
		return $this->fieldName;
	}

	public function getType(): BucketValueType {
		return $this->type;
	}

	public function getIndexed(): bool {
		return $this->indexed;
	}

	public function getRepeated(): bool {
		return $this->repeated;
	}

	public function jsonSerialize(): array {
		return [
			'type' => $this->getType()->value,
			'index' => $this->getIndexed(),
			'repeated' => $this->getRepeated()
		];
	}

	/**
	 * Example json: {"type":"INTEGER","index":false,"repeated":false}
	 */
	public static function fromJson( string $fieldName, string $json ): BucketSchemaField {
		$jsonRow = json_decode( $json, true );
		// This is the old style (Pre-July 2025) of table comment formatting.
		// Example json: {"_page_id":{"type":"INTEGER","index":false,"repeated":false}}
		// This can be removed once old buckets are no longer used (after Bucket is in prod)
		if ( isset( $jsonRow[$fieldName]['type'] ) ) {
			$jsonRow = $jsonRow[$fieldName];
		}
		// End old style handling
		return new BucketSchemaField(
			$fieldName, BucketValueType::from( $jsonRow['type'] ), $jsonRow['index'], $jsonRow['repeated'] );
	}

	public function getDatabaseValueType(): DatabaseValueType {
		if ( $this->getRepeated() ) {
			return DatabaseValueType::Json;
		}
		return $this->getSubDatabaseValueType();
	}

	public function getSubDatabaseValueType(): DatabaseValueType {
		switch ( $this->getType() ) {
			case BucketValueType::Text:
			case BucketValueType::Page:
				return DatabaseValueType::Text;
			case BucketValueType::Boolean:
				return DatabaseValueType::Boolean;
			case BucketValueType::Double:
				return DatabaseValueType::Double;
			case BucketValueType::Integer:
				return DatabaseValueType::Integer;
			default:
				return DatabaseValueType::Text;
		}
	}

	public function castValueForDatabase( mixed $value ): mixed {
		if ( $this->getRepeated() ) {
			if ( !is_array( $value ) ) {
				// Wrap single values in an array for compatability
				$value = [ $value ];
			}
			$value = array_values( $value );
			$outputValues = [];
			$totalLength = 0;
			foreach ( $value as $single ) {
				if ( $single === null ) {
					continue;
				}
				$single = strval( $single );
				$outputValues[] = $single;
				if ( strlen( $single ) > Bucket::REPEATED_CHARACTER_LIMIT ) {
					throw new BucketException( wfMessage( 'bucket-put-repeated-too-long' )
							->numParams( Bucket::REPEATED_CHARACTER_LIMIT ) );
				}
				$totalLength = $totalLength + strlen( $single );
			}
			if ( $totalLength > Bucket::REPEATED_CHARACTER_TOTAL_LIMIT ) {
				throw new BucketException( wfMessage( 'bucket-put-repeated-total-too-long' )
					->numParams( $totalLength, Bucket::REPEATED_CHARACTER_TOTAL_LIMIT ) );
			}
			if ( count( $outputValues ) === 0 ) {
				return null;
			}
			$jsonValue = json_encode( $outputValues );
			if ( strlen( $jsonValue ) > Bucket::TEXT_BYTE_LIMIT ) {
				throw new BucketException( wfMessage( 'bucket-put-repeated-total-too-long' )
					->numParams( strlen( $jsonValue ), Bucket::TEXT_BYTE_LIMIT ) );
			}
			return $jsonValue;
		}
		return $this->castSubValueForDatabase( $value );
	}

	public function castSubValueForDatabase( mixed $value ): mixed {
		if ( $value === null ) {
			return null;
		}
		switch ( $this->getSubDatabaseValueType() ) {
			case DatabaseValueType::Text:
				if ( is_array( $value ) ) {
					$value = json_encode( $value );
				} else {
					$value = strval( $value );
				}
				if ( strlen( $value ) > Bucket::TEXT_BYTE_LIMIT ) {
					throw new BucketException( wfMessage( 'bucket-put-text-too-long' )
						->numParams( strlen( $value ), Bucket::TEXT_BYTE_LIMIT ) );
				}
				return $value;
			case DatabaseValueType::Double:
				return floatval( $value );
			case DatabaseValueType::Integer:
				return intval( $value );
			case DatabaseValueType::Boolean:
				// MySQL uses 1 for true, 0 for false
				return (int)filter_var( $value, FILTER_VALIDATE_BOOL );
			default:
				return null;
		}
	}

	/**
	 * @param mixed $value
	 * @return array|bool|float|int|void
	 */
	public function castValueForLua( $value ) {
		if ( $value === null ) {
			return null;
		}
		$type = $this->getType();
		if ( $this->getRepeated() ) {
			$ret = [];
			$jsonData = json_decode( $value, true );
			// If we are in a repeated field but only holding a scalar, make it an array anyway.
			if ( !is_array( $jsonData ) ) {
				$jsonData = [ $jsonData ];
			}
			$nonRepeatedData = new BucketSchemaField(
				$this->getFieldName(), $this->getType(), $this->getIndexed(), false );
			foreach ( $jsonData as $subVal ) {
				$ret[] = $nonRepeatedData->castValueForLua( $subVal );
			}
			return $ret;
		}
		switch ( $type ) {
			case BucketValueType::Text:
			case BucketValueType::Page:
				return $value;
			case BucketValueType::Double:
				return floatval( $value );
			case BucketValueType::Integer:
				return intval( $value );
			case BucketValueType::Boolean:
				return boolval( $value );
		}
	}
}

class BucketException extends LogicException {
	private Message $wfMessage;

	public function __construct( Message $wfMessage ) {
		parent::__construct( $wfMessage );
		$this->wfMessage = $wfMessage;
	}

	public function getwfMessage(): Message {
		return $this->wfMessage;
	}
}

class SchemaException extends BucketException {
}

class QueryException extends BucketException {
}
