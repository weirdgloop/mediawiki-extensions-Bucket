<?php

namespace MediaWiki\Extension\Bucket;

use JsonSerializable;
use LogicException;
use Wikimedia\Rdbms\IDatabase;
use MediaWiki\MediaWikiServices;

class Bucket {
	public const EXTENSION_DATA_KEY = 'bucket:puts';
	public const MESSAGE_BUCKET = 'bucket_message';
	public const REPEATED_CHARACTER_LIMIT = 512;

	/**
	 * Cannot be static because RefreshLinks job will run on multiple pages
	 */
	private array $logs = [];
	private array $newPuts = [];

	public function logMessage( string $bucket, string $property, string $type, string $message ): void {
		if ( $bucket !== '' ) {
			$bucket = 'Bucket:' . $bucket;
		}
		$this->logs[] = [
			'sub' => '',
			'data' => [
				'bucket' => $bucket,
				'property' => $property,
				'type' => wfMessage( $type ),
				'message' => $message
			]
		];
	}

	/**
	 * Called when a page is saved containing a bucket.put
	 */
	public function writePuts( int $pageId, string $titleText, array $puts, bool $writingLogs = false ): void {
		$dbw = BucketDatabase::getDB();
		if ( !$writingLogs ) {
			$putLength = 0;
			$maxiumPutLength = MediaWikiServices::getInstance()->getMainConfig()->get('BucketMaxDataPerPage');
		}

		$res = $dbw->newSelectQueryBuilder()
				->from( 'bucket_pages' )
				->select( [ 'bucket_name', 'put_hash' ] )
				->forUpdate()
				->where( [ '_page_id' => $pageId ] )
				->caller( __METHOD__ )
				->fetchResultSet();
		$bucket_hash = [];
		foreach ( $res as $row ) {
			$bucket_hash[ $row->bucket_name ] = $row->put_hash;
		}

		$schemas = [];

		if ( count( $puts ) > 0 ) {
			// Combine existing written bucket list and new written bucket list.
			$relevantBuckets = array_merge( array_keys( $puts ), array_keys( $bucket_hash ) );
			$res = $dbw->newSelectQueryBuilder()
					->from( 'bucket_schemas' )
					->select( [ 'bucket_name', 'schema_json' ] )
					->lockInShareMode()
					->where( [ 'bucket_name' => $relevantBuckets ] )
					->caller( __METHOD__ )
					->fetchResultSet();
			foreach ( $res as $row ) {
				$schemas[$row->bucket_name] = new BucketSchema(
					$row->bucket_name, json_decode( $row->schema_json, true ) );
			}
		}

		foreach ( $puts as $bucketName => $bucketData ) {
			if ( $bucketName === '' ) {
				self::logMessage(
					$bucketName, '', 'bucket-general-error', wfMessage( 'bucket-no-bucket-defined-warning' ) );
				continue;
			}

			try {
				$bucketNameTmp = self::getValidFieldName( $bucketName );
			} catch ( SchemaException ) {
				self::logMessage(
					$bucketName, '', 'bucket-general-warning', wfMessage(
						'bucket-invalid-name-warning', $bucketName ) );
				continue;
			}

			if ( $bucketNameTmp !== $bucketName ) {
				self::logMessage(
					$bucketName, '', 'bucket-general-warning', wfMessage( 'bucket-capital-name-warning' ) );
			}
			$bucketName = $bucketNameTmp;

			if ( $bucketName === self::MESSAGE_BUCKET && $writingLogs === false ) {
				self::logMessage(
					$bucketName, self::MESSAGE_BUCKET, 'bucket-general-error', wfMessage(
						'bucket-cannot-write-to-system-bucket' ) );
				continue;
			}

			if ( !array_key_exists( $bucketName, $schemas ) ) {
				self::logMessage( $bucketName, '', 'bucket-general-error', wfMessage( 'bucket-no-exist-error' ) );
				continue;
			}
			$bucketSchema = $schemas[$bucketName];

			$tablePuts = [];
			$dbTableName = BucketDatabase::getBucketTableName( $bucketName );
			$res = $dbw->newSelectQueryBuilder()
				->from( $dbw->addIdentifierQuotes( $dbTableName ) )
				->select( '*' )
				->forUpdate()
				->where( [ '_page_id' => $pageId ] )
				->caller( __METHOD__ )
				->fetchResultSet();

			$fields = [];
			$fieldNames = $res->getFieldNames();
			foreach ( $fieldNames as $fieldName ) {
				// If the table has a field that isn't present in the schema, the schema must be out of date.
				if ( !isset( $bucketSchema->getFields()[$fieldName] ) ) {
					self::logMessage(
						$bucketName, $fieldName, 'bucket-general-error', wfMessage( 'bucket-schema-outdated-error' ) );
				} else {
					$fields[$fieldName] = true;
				}
			}
			foreach ( $bucketData as $idx => $singleData ) {
				$sub = $singleData['sub'];
				$singleData = $singleData['data'];
				if ( !is_array( $singleData ) ) {
					self::logMessage( $bucketName, '', 'bucket-general-error', wfMessage( 'bucket-put-syntax-error' ) );
					continue;
				}
				foreach ( $singleData as $key => $value ) {
					if ( !isset( $fields[$key] ) || !$fields[$key] ) {
						self::logMessage(
							$bucketName, $key, 'bucket-general-warning', wfMessage(
								'bucket-put-key-missing-warning', $key, $bucketName ) );
					}
				}
				$singlePut = [];
				foreach ( $fields as $key => $_ ) {
					$value = $singleData[$key] ?? null;
					try {
						$singlePut[$dbw->addIdentifierQuotes( $key )] =
							$bucketSchema->getField( $key )->castValueForDatabase( $value );
					} catch ( BucketException $e ) {
						self::logMessage(
							$bucketName, $key, 'bucket-general-error', wfMessage(
								$e->getMessage(), self::REPEATED_CHARACTER_LIMIT ) );
					}
				}
				$singlePut[$dbw->addIdentifierQuotes( '_page_id' )] = $pageId;
				$singlePut[$dbw->addIdentifierQuotes( '_index' )] = $idx;
				$singlePut[$dbw->addIdentifierQuotes( 'page_name' )] = $titleText;
				$singlePut[$dbw->addIdentifierQuotes( 'page_name_sub' )] = $titleText;
				if ( isset( $sub ) && strlen( $sub ) > 0 ) {
					$singlePut[$dbw->addIdentifierQuotes( 'page_name_sub' )] = $titleText . '#' . $sub;
				}
				$tablePuts[$idx] = $singlePut;
			}

			# Check these puts against the hash of the last time we did puts.
			sort( $tablePuts );
			$tableJson = json_encode( $tablePuts );
			$putLength += strlen($tableJson);
			$newHash = hash( 'sha256', $tableJson . json_encode( $bucketSchema ) );
			if ( isset( $bucket_hash[ $bucketName ] ) && $bucket_hash[ $bucketName ] === $newHash ) {
				unset( $bucket_hash[ $bucketName ] );
				continue;
			}

			// Remove the bucket_hash entry so we can use $bucket_hash as a list of removed buckets at the end.
			unset( $bucket_hash[ $bucketName ] );
			if ( $putLength <= $maxiumPutLength || $writingLogs ) {
				$this->newPuts[$bucketName] =
					['_page_id' => $pageId, 'bucket_name' => $bucketName, 'put_hash' => $newHash];

				$dbw->newDeleteQueryBuilder()
					->deleteFrom($dbw->addIdentifierQuotes($dbTableName))
					->where(['_page_id' => $pageId])
					->caller(__METHOD__)
					->execute();
				$dbw->newInsertQueryBuilder()
					->insert($dbw->addIdentifierQuotes($dbTableName))
					->rows($tablePuts)
					->caller(__METHOD__)
					->execute();
			}
		}

		if ( $writingLogs ) {
			return;
		}

		if ($putLength > $maxiumPutLength) {
			self::logMessage($bucketName, '', 'bucket-general-error', wfMessage('bucket-put-total-too-long', $putLength, $maxiumPutLength));
		}

		if ( count( $this->logs ) > 0 ) {
			self::writePuts( $pageId, $titleText, [ self::MESSAGE_BUCKET => $this->logs ], true );
			unset( $bucket_hash[self::MESSAGE_BUCKET] );
		}

		// Insert new/updated hashes to bucket_pages
		if ( count( $this->newPuts ) > 0 ) {
			$dbw->newReplaceQueryBuilder()
				->replaceInto( 'bucket_pages' )
				->uniqueIndexFields( [ '_page_id', 'bucket_name' ] )
				->rows( array_values( $this->newPuts ) )
				->caller( __METHOD__ )
				->execute();
		}

		// Clean up bucket_pages entries for buckets that are no longer written to on this page.
		$tablesToDelete = array_keys( $bucket_hash );
		if ( count( $tablesToDelete ) > 0 ) {
			$dbw->newDeleteQueryBuilder()
				->deleteFrom( 'bucket_pages' )
				->where( [ '_page_id' => $pageId, 'bucket_name' => $tablesToDelete ] )
				->caller( __METHOD__ )
				->execute();
			foreach ( $tablesToDelete as $name ) {
				$dbw->newDeleteQueryBuilder()
					->deleteFrom( BucketDatabase::getBucketTableName( $name ) )
					->where( [ '_page_id' => $pageId ] )
					->caller( __METHOD__ )
					->execute();
			}
		}
	}

	public static function getValidFieldName( ?string $fieldName ): string {
		if ( $fieldName !== null
			// Disallow numeric field names as the MW RDBMS treats numeric tables names as ints in some circumstances.
			&& is_numeric( $fieldName ) === false
			&& !str_starts_with( $fieldName, '_' )
			&& preg_match( '/^[a-zA-Z0-9_]+$/D', $fieldName ) ) {
			$cleanName = strtolower( trim( $fieldName ) );
			// MySQL has a maximum of 64, lets limit it to 60 in case we need to append to fields for some reason later
			if ( strlen( $cleanName ) <= 60 ) {
				return $cleanName;
			}
		}
		throw new SchemaException( wfMessage( 'bucket-schema-invalid-field-name', $fieldName ) );
	}

	public static function getValidBucketName( string $bucketName ): string {
		if ( ucfirst( $bucketName ) !== ucfirst( strtolower( $bucketName ) ) ) {
			throw new SchemaException( wfMessage( 'bucket-capital-name-error' ) );
		}
		try {
			return self::getValidFieldName( $bucketName );
		} catch ( SchemaException ) {
			throw new SchemaException( wfMessage( 'bucket-invalid-name-warning', $bucketName ) );
		}
	}

	/**
	 * @return int - The number of pages writing to this bucket
	 */
	public static function countPagesUsingBucket( string $bucketName ): int {
		$dbw = BucketDatabase::getDB();
		$bucketName = self::getValidBucketName( $bucketName );
		return $dbw->newSelectQueryBuilder()
						->table( 'bucket_pages' )
						->lockInShareMode()
						->where( [ 'bucket_name' => $bucketName ] )
						->fetchRowCount();
	}

	/**
	 * @param array $userInput
	 * @param bool $changedPage - true if the current page is different than the last parsed page
	 * @return array
	 */
	public static function runSelect( array $userInput, bool $changedPage ): array {
		$query = new BucketQuery( $userInput, $changedPage );
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
		return [ $result, $sql_string ];
	}
}

enum ValueType: string {
	case Text = 'TEXT';
	case Page = 'PAGE';
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

	public function __construct( string $bucketName, array $schema, int $timestamp = 0 ) {
		$this->timestamp = $timestamp;
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
					ValueType::from( $val['type'] ),
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
	private ValueType $type;
	private bool $indexed;
	private bool $repeated;

	public function __construct( string $fieldName, ValueType $type, bool $indexed, bool $repeated ) {
		$this->fieldName = $fieldName;
		$this->type = $type;
		$this->indexed = $indexed;
		$this->repeated = $repeated;
	}

	public function getFieldName(): string {
		return $this->fieldName;
	}

	public function getType(): ValueType {
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
			$fieldName, ValueType::from( $jsonRow['type'] ), $jsonRow['index'], $jsonRow['repeated'] );
	}

	/**
	 * The ValueType that this field is stored as in the database.
	 */
	public function getDatabaseValueType(): ValueType {
		if ( $this->getRepeated() ) {
			return ValueType::Json;
		} else {
			// Page is just stored as text in the database
			if ( $this->getType() === ValueType::Page ) {
				return ValueType::Text;
			}
			return $this->getType();
		}
	}

	public function castValueForDatabase( mixed $value ): mixed {
		if ( $value === null ) {
			return null;
		}
		switch ( $this->getDatabaseValueType() ) {
			case ValueType::Text:
			case ValueType::Page:
				if ( $value === '' ) {
					return null;
				} else {
					if ( is_array( $value ) ) {
						return json_encode( $value );
					}
					return $value;
				}
			case ValueType::Double:
				return floatval( $value );
			case ValueType::Integer:
				return intval( $value );
			case ValueType::Boolean:
				// MySQL uses 1 for true, 0 for false
				return (int)filter_var( $value, FILTER_VALIDATE_BOOL );
			case ValueType::Json:
				if ( !is_array( $value ) ) {
					if ( $value === '' ) {
						return null;
					} else {
						// Wrap single values in an array for compatability
						$value = [ $value ];
					}
				}
				$value = array_values( $value );
				if ( count( $value ) > 0 ) {
					foreach ( $value as $single ) {
						// Repeated fields can only store up to 512 characters in an individual value
						if ( is_string( $single ) && strlen( $single ) > Bucket::REPEATED_CHARACTER_LIMIT ) {
							throw new BucketException( 'bucket-put-repeated-too-long' );
						}
					}
					return json_encode( $value );
				} else {
					return null;
				}
		}
		return null;
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
			if ( $value === null ) {
				$value = '';
			}
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
		} elseif ( $type === ValueType::Text || $type === ValueType::Page ) {
			return $value;
		} elseif ( $type === ValueType::Double ) {
			return floatval( $value );
		} elseif ( $type === ValueType::Integer ) {
			return intval( $value );
		} elseif ( $type === ValueType::Boolean ) {
			return boolval( $value );
		}
	}
}

class BucketException extends LogicException {
}

class SchemaException extends BucketException {
}

class QueryException extends BucketException {
}
