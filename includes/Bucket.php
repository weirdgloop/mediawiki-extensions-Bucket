<?php

namespace MediaWiki\Extension\Bucket;

use JsonSerializable;
use LogicException;
use MediaWiki\MediaWikiServices;
use Message;
use Wikimedia\Rdbms\IDatabase;
use Wikimedia\Rdbms\IMaintainableDatabase;

class Bucket {
	public const string EXTENSION_DATA_KEY = 'bucket:puts';
	public const string MESSAGE_BUCKET = 'bucket_message';

	private static array $logs = [];
	private static IMaintainableDatabase $db;
	private static bool $specialBucketUser = false;

	public static function getDB(): IMaintainableDatabase {
		if ( isset( self::$db ) && self::$db->isOpen() ) {
			return self::$db;
		}
		$config = MediaWikiServices::getInstance()->getMainConfig();
		$bucketDBuser = $config->get( 'BucketDBuser' );
		$bucketDBpassword = $config->get( 'BucketDBpassword' );

		$mainDB = self::getMainDB();
		if ( $bucketDBuser == null || $bucketDBpassword == null ) {
			// TODO need to set utf8Mode for this if you want to be able to store repeated fields
			self::$db = $mainDB;
			self::$specialBucketUser = false;
			return self::$db;
		}

		$params = [
			'host' => $mainDB->getServer(),
			'user' => $bucketDBuser,
			'password' => $bucketDBpassword,
			'dbname' => $mainDB->getDBname(),
			'utf8Mode' => true
		];

		self::$db = MediaWikiServices::getInstance()->getDatabaseFactory()->create( $mainDB->getType(), $params );
		self::$specialBucketUser = true;
		return self::$db;
	}

	private static function getMainDB(): IMaintainableDatabase {
		// Note: Cannot be used to write Bucket data due to json requiring a utf8 connection
		return MediaWikiServices::getInstance()->getDBLoadBalancer()->getConnection( DB_PRIMARY );
	}

	private static function getBucketDBUser(): string {
		$config = MediaWikiServices::getInstance()->getMainConfig();
		$dbUser = $config->get( 'BucketDBuser' );
		$dbServer = $config->get( 'BucketDBserver' );
		return "$dbUser@'$dbServer'";
	}

	public static function logMessage( string $bucket, string $property, string $type, string $message ): void {
		if ( $bucket != '' ) {
			$bucket = 'Bucket:' . $bucket;
		}
		self::$logs[] = [
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
	 * @property BucketSchema[] $schemas
	 */
	public static function writePuts( int $pageId, string $titleText, array $puts, bool $writingLogs = false ): void {
		$dbw = self::getDB();

		$res = $dbw->newSelectQueryBuilder()
				->from( 'bucket_pages' )
				->select( [ '_page_id', 'bucket_name', 'put_hash' ] )
				->forUpdate()
				->where( [ '_page_id' => $pageId ] )
				->caller( __METHOD__ )
				->fetchResultSet();
		$bucket_hash = [];
		foreach ( $res as $row ) {
			$bucket_hash[ $row->bucket_name ] = $row->put_hash;
		}

		// Combine existing written bucket list and new written bucket list.
		$relevantBuckets = array_merge( array_keys( $puts ), array_keys( $bucket_hash ) );
		$res = $dbw->newSelectQueryBuilder()
				->from( 'bucket_schemas' )
				->select( [ 'bucket_name', 'schema_json' ] )
				->lockInShareMode()
				->where( [ 'bucket_name' => $relevantBuckets ] )
				->caller( __METHOD__ )
				->fetchResultSet();
		$schemas = [];
		foreach ( $res as $row ) {
			$schemas[$row->bucket_name] = new BucketSchema( $row->bucket_name, json_decode( $row->schema_json, false ) );
		}

		foreach ( $puts as $bucketName => $bucketData ) {
			if ( $bucketName == '' ) {
				self::logMessage( $bucketName, '', 'bucket-general-error', wfMessage( 'bucket-no-bucket-defined-warning' ) );
				continue;
			}

			try {
				$bucketNameTmp = self::getValidFieldName( $bucketName );
			} catch ( SchemaException $e ) {
				self::logMessage( $bucketName, '', 'bucket-general-warning', wfMessage( 'bucket-invalid-name-warning', $bucketName ) );
				continue;
			}

			if ( $bucketNameTmp != $bucketName ) {
				self::logMessage( $bucketName, '', 'bucket-general-warning', wfMessage( 'bucket-capital-name-warning' ) );
			}
			$bucketName = $bucketNameTmp;

			if ( $bucketName == self::MESSAGE_BUCKET && $writingLogs == false ) {
				self::logMessage( $bucketName, self::MESSAGE_BUCKET, 'bucket-general-error', wfMessage( 'bucket-cannot-write-to-system-bucket' ) );
				continue;
			}

			if ( !array_key_exists( $bucketName, $schemas ) ) {
				self::logMessage( $bucketName, '', 'bucket-general-error', wfMessage( 'bucket-no-exist-error' ) );
				continue;
			}
			$bucketSchema = $schemas[$bucketName];

			$tablePuts = [];
			$dbTableName = self::getBucketTableName( $bucketName );
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
					self::logMessage( $bucketName, $fieldName, 'bucket-general-error', wfMessage( 'bucket-schema-outdated-error' ) );
				} else {
					$fields[$fieldName] = true;
				}
			}
			foreach ( $bucketData as $idx => $singleData ) {
				$sub = $singleData['sub'];
				$singleData = $singleData['data'];
				if ( gettype( $singleData ) != 'array' ) {
					self::logMessage( $bucketName, '', 'bucket-general-error', wfMessage( 'bucket-put-syntax-error' ) );
					continue;
				}
				foreach ( $singleData as $key => $value ) {
					if ( !isset( $fields[$key] ) || !$fields[$key] ) {
						self::logMessage( $bucketName, $key, 'bucket-general-warning', wfMessage( 'bucket-put-key-missing-warning', $key, $bucketName ) );
					}
				}
				$singlePut = [];
				foreach ( $fields as $key => $_ ) {
					$value = isset( $singleData[$key] ) ? $singleData[$key] : null;
					$singlePut[$dbw->addIdentifierQuotes( $key )] = $bucketSchema->getField( $key )->castValueForDatabase( $value );
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
			$newHash = hash( 'sha256', json_encode( $tablePuts ) . json_encode( $bucketSchema ) );
			if ( isset( $bucket_hash[ $bucketName ] ) && $bucket_hash[ $bucketName ] == $newHash ) {
				unset( $bucket_hash[ $bucketName ] );
				continue;
			}

			// Remove the bucket_hash entry so we can it as a list of removed buckets at the end.
			unset( $bucket_hash[ $bucketName ] );

			$dbw->newDeleteQueryBuilder()
				->deleteFrom( $dbw->addIdentifierQuotes( $dbTableName ) )
				->where( [ '_page_id' => $pageId ] )
				->caller( __METHOD__ )
				->execute();
			$dbw->newInsertQueryBuilder()
				->insert( $dbw->addIdentifierQuotes( $dbTableName ) )
				->rows( $tablePuts )
				->caller( __METHOD__ )
				->execute();
			$dbw->newDeleteQueryBuilder()
				->deleteFrom( 'bucket_pages' )
				->where( [ '_page_id' => $pageId, 'bucket_name' => $bucketName ] )
				->caller( __METHOD__ )
				->execute();
			$dbw->newInsertQueryBuilder()
				->insert( 'bucket_pages' )
				->rows( [ '_page_id' => $pageId, 'bucket_name' => $bucketName, 'put_hash' => $newHash ] )
				->caller( __METHOD__ )
				->execute();
		}

		if ( !$writingLogs ) {
			// Clean up bucket_pages entries for buckets that are no longer written to on this page.
			$tablesToDelete = array_keys( $bucket_hash );
			if ( count( self::$logs ) != 0 ) {
				unset( $tablesToDelete[self::MESSAGE_BUCKET] );
			} else {
				$tablesToDelete[] = self::MESSAGE_BUCKET;
			}

			if ( count( $tablesToDelete ) > 0 ) {
				$dbw->newDeleteQueryBuilder()
					->deleteFrom( 'bucket_pages' )
					->where( [ '_page_id' => $pageId, 'bucket_name' => $tablesToDelete ] )
					->caller( __METHOD__ )
					->execute();
				foreach ( $tablesToDelete as $name ) {
					$dbw->newDeleteQueryBuilder()
						->deleteFrom( self::getBucketTableName( $name ) )
						->where( [ '_page_id' => $pageId ] )
						->caller( __METHOD__ )
						->execute();
				}
			}

			if ( count( self::$logs ) > 0 ) {
				self::writePuts( $pageId, $titleText, [ self::MESSAGE_BUCKET => self::$logs ], true );
			}
		}
	}

	/**
	 * Called for any page save that doesn't have bucket puts
	 */
	public static function clearOrphanedData( int $pageId ): void {
		$dbw = self::getDB();

		// Check if any buckets are storing data for this page
		$res = $dbw->newSelectQueryBuilder()
				->from( 'bucket_pages' )
				->select( [ 'bucket_name' ] )
				->forUpdate()
				->where( [ '_page_id' => $pageId ] )
				->groupBy( 'bucket_name' )
				->caller( __METHOD__ )
				->fetchResultSet();

		// If there is data associated with this page, delete it.
		if ( $res->count() > 0 ) {
			$dbw->newDeleteQueryBuilder()
				->deleteFrom( 'bucket_pages' )
				->where( [ '_page_id' => $pageId ] )
				->caller( __METHOD__ )
				->execute();
			$table = [];
			foreach ( $res as $row ) {
				$table[] = $row->bucket_name;
			}

			foreach ( $table as $name ) {
				// Clear this pages data from the bucket
				$dbw->newDeleteQueryBuilder()
					->deleteFrom( self::getBucketTableName( $name ) )
					->where( [ '_page_id' => $pageId ] )
					->caller( __METHOD__ )
					->execute();
			}
		}
	}

	public static function getValidFieldName( ?string $fieldName ): string {
		if ( $fieldName != null && preg_match( '/^[a-zA-Z0-9_]+$/', $fieldName ) ) {
			$cleanName = strtolower( trim( $fieldName ) );
			// MySQL has a maximum of 64, lets limit it to 60 in case we need to append to fields for some reason later
			if ( strlen( $cleanName ) <= 60 ) {
				return $cleanName;
			}
		}
		throw new SchemaException( wfMessage( 'bucket-schema-invalid-field-name', $fieldName ) );
	}

	public static function getValidBucketName( string $bucketName ): string {
		if ( ucfirst( $bucketName ) != ucfirst( strtolower( $bucketName ) ) ) {
			throw new SchemaException( wfMessage( 'bucket-capital-name-error' ) );
		}
		try {
			return self::getValidFieldName( $bucketName );
		} catch ( SchemaException $e ) {
			throw new SchemaException( wfMessage( 'bucket-invalid-name-warning', $bucketName ) );
		}
	}

	public static function canCreateTable( string $bucketName ): bool {
		$bucketName = self::getValidBucketName( $bucketName );
		$dbw = self::getDB();
		$schemaExists = $dbw->newSelectQueryBuilder()
			->from( 'bucket_schemas' )
			->where( [ 'bucket_name' => $bucketName ] )
			->forUpdate()
			->caller( __METHOD__ )
			->field( 'schema_json' )
			->fetchField();
		$tableExists = $dbw->tableExists( self::getBucketTableName( $bucketName ) );
		if ( !$schemaExists && !$tableExists ) {
			return true;
		} else {
			return false;
		}
	}

	/**
	 * The table comments hold a json representation of the applied Bucket schema
	 * Example comment for field _page_id: {"type":"INTEGER","index":false,"repeated":false}
	 */
	private static function buildSchemaFromComments( string $bucketName, IDatabase $dbw ): BucketSchema {
		$dbTableName = self::getBucketTableName( $bucketName );
		$res = $dbw->query( "SHOW FULL COLUMNS FROM $dbTableName;", __METHOD__ );

		$fields = [];
		foreach ( $res as $row => $val ) {
			$fields[] = BucketSchemaField::fromJson( $val->Field, $val->Comment );
		}
		return new BucketSchema( $bucketName, $fields, time() );
	}

	public static function createOrModifyTable( string $bucketName, object $jsonSchema, bool $isExistingPage ): void {
		$bucketName = self::getValidBucketName( $bucketName );
		$newSchema = [
			'_page_id' => new BucketSchemaField( '_page_id', ValueType::Integer, false, false ),
			'_index' => new BucketSchemaField( '_index', ValueType::Integer, false, false ),
			'page_name' => new BucketSchemaField( 'page_name', ValueType::Page, true, false ),
			'page_name_sub' => new BucketSchemaField( 'page_name_sub', ValueType::Page, true, false )
		];

		if ( $bucketName == self::MESSAGE_BUCKET ) {
			throw new SchemaException( wfMessage( 'bucket-cannot-create-system-page' ) );
		}

		if ( !$isExistingPage && !self::canCreateTable( $bucketName ) ) {
			throw new SchemaException( wfMessage( 'bucket-already-exist-error' ) );
		}

		if ( empty( (array)$jsonSchema ) ) {
			throw new SchemaException( wfMessage( 'bucket-schema-no-fields-error' ) );
		}

		foreach ( $jsonSchema as $fieldName => $fieldData ) {
			if ( gettype( $fieldName ) !== 'string' ) {
				throw new SchemaException( wfMessage( 'bucket-schema-must-be-strings', $fieldName ) );
			}

			$lcFieldName = self::getValidFieldName( $fieldName );

			if ( isset( $newSchema[$lcFieldName] ) ) {
				throw new SchemaException( wfMessage( 'bucket-schema-duplicated-field-name', $fieldName ) );
			}

			$valueType = ValueType::tryFrom( $fieldData->type );
			if ( $valueType == null ) {
				throw new SchemaException( wfMessage( 'bucket-schema-invalid-data-type', $fieldName, $fieldData->type ) );
			}

			$index = true;
			if ( isset( $fieldData->index ) ) {
				$index = boolval( $fieldData->index );
			}

			$repeated = false;
			if ( isset( $fieldData->repeated ) ) {
				$repeated = boolval( $fieldData->repeated );
			}

			if ( $repeated == true && $index == false ) {
				throw new SchemaException( wfMessage( 'bucket-schema-repeated-must-be-indexed', $fieldName ) );
			}

			$newSchema[$lcFieldName] = new BucketSchemaField( $lcFieldName, $valueType, $index, $repeated );
		}

		if ( count( $newSchema ) > 64 ) {
			throw new SchemaException( wfMessage( 'bucket-schema-too-many-fields' ) );
		}

		$bucketSchema = new BucketSchema( $bucketName, $newSchema );
		$dbw = self::getDB();

		$dbw->onTransactionCommitOrIdle( function () use ( $dbw, $bucketSchema ) {
			if ( !$dbw->tableExists( $bucketSchema->getTableName(), __METHOD__ ) ) {
				// We are a new bucket json
				$statement = self::getCreateTableStatement( $bucketSchema, $dbw );
				// Grant perms to the new table
				if ( self::$specialBucketUser ) {
					$bucketDBuser = self::getBucketDBUser();
					$mainDB = self::getMainDB();
					$mainDB->query( $statement );
					$escapedTableName = $bucketSchema->getQuotedTableName( $dbw );
					$mainDB->query( "GRANT ALL ON $escapedTableName TO $bucketDBuser;" );
				} else {
					$dbw->query( $statement );
				}
			} else {
				// We are an existing bucket json
				$oldSchema = self::buildSchemaFromComments( $bucketSchema->getName(), $dbw );
				$statement = self::getAlterTableStatement( $bucketSchema, $oldSchema, $dbw );
				$dbw->query( $statement );
			}

			// At this point is is possible that another transaction has changed the table
			//So we start a transaction, read the column comments (which are the schema), and write that to bucket_schemas
			$dbw->begin( __METHOD__ );
			$schemaJson = self::buildSchemaFromComments( $bucketSchema->getName(), $dbw );
			$schemaJson = json_encode( $schemaJson );
			$dbw->upsert(
				'bucket_schemas',
				[ 'bucket_name' => $bucketSchema->getName(), 'schema_json' => $schemaJson ],
				'bucket_name',
				[ 'schema_json' => $schemaJson ]
			);
			$dbw->commit( __METHOD__ );
		}, __METHOD__ );
	}

	private static function getAlterTableStatement( BucketSchema $bucketSchema, BucketSchema $oldSchema, IDatabase $dbw ): string {
		$alterTableFragments = [];

		$oldFields = $oldSchema->getFields();

		$previousColumn = null;
		foreach ( $bucketSchema->getFields() as $fieldName => $field ) {
			$escapedFieldName = $dbw->addIdentifierQuotes( $fieldName );
			$fieldJson = $dbw->addQuotes( json_encode( $field ) );
			if ( isset( $oldFields[$fieldName] ) ) {
				$oldDbType = $oldFields[$fieldName]->getDatabaseValueType()->value;
			}
			$after = '';
			if ( isset( $previousColumn ) ) {
				$after = " AFTER {$dbw->addIdentifierQuotes($previousColumn)}";
			}
			$newDbType = $field->getDatabaseValueType()->value;
			# Handle new fields
			if ( !isset( $oldFields[$fieldName] ) ) {
				$alterTableFragments[] = "ADD $escapedFieldName " . $newDbType . " COMMENT $fieldJson" . $after;
				if ( $field->getIndexed() ) {
					$alterTableFragments[] = 'ADD ' . self::getIndexStatement( $field, $dbw );
				}
			# Handle type changes, including add/drop index
			} elseif ( $oldDbType !== $newDbType ) {
				if ( $oldFields[$fieldName]->getIndexed() ) {
					$alterTableFragments[] = "DROP INDEX $escapedFieldName";
				}
				$alterTableFragments[] = "DROP $escapedFieldName"; # Always drop and then re-add the column for field type changes.
				$alterTableFragments[] = "ADD $escapedFieldName " . $newDbType . " COMMENT $fieldJson" . $after;
				if ( $field->getIndexed() ) {
					$alterTableFragments[] = 'ADD ' . self::getIndexStatement( $field, $dbw );
				}
			# Handle adding index without type change
			} elseif ( ( $oldFields[$fieldName]->getIndexed() === false && $field->getIndexed() === true ) ) {
				$alterTableFragments[] = "MODIFY $escapedFieldName " . $newDbType . " COMMENT $fieldJson"; // Acts as a no-op except to set the comment
				$alterTableFragments[] = 'ADD ' . self::getIndexStatement( $field, $dbw );
			# Handle removing index
			} elseif ( ( $oldFields[$fieldName]->getIndexed() === true && $field->getIndexed() === false ) ) {
				$alterTableFragments[] = "MODIFY $escapedFieldName " . $newDbType . " COMMENT $fieldJson"; // Acts as a no-op except to set the comment
				$alterTableFragments[] = "DROP INDEX $escapedFieldName";
			# Handle changing between types that don't actually change the DB type
			} elseif ( ( $oldFields[$fieldName]->getType() != $field->getType() ) ) {
				$alterTableFragments[] = "MODIFY $escapedFieldName " . $newDbType . " COMMENT $fieldJson"; // Acts as a no-op except to set the comment
			}
			unset( $oldFields[$fieldName] );
			$previousColumn = $fieldName;
		}
		// Drop unused columns
		foreach ( $oldFields as $deletedColumn => $val ) {
			$escapedDeletedColumn = $dbw->addIdentifierQuotes( $deletedColumn );
			if ( $val->getRepeated() === true ) {
				$alterTableFragments[] = "DROP INDEX $escapedDeletedColumn"; // We must explicitly drop indexes for repeated fields
			}
			$alterTableFragments[] = "DROP $escapedDeletedColumn";
		}

		$dbTableName = $dbw->addIdentifierQuotes( $bucketSchema->getTableName() );
		return "ALTER TABLE $dbTableName " . implode( ', ', $alterTableFragments ) . ';';
	}

	private static function getCreateTableStatement( BucketSchema $newSchema, IDatabase $dbw ): string {
		$createTableFragments = [];

		foreach ( $newSchema->getFields() as $field ) {
			$dbType = $field->getDatabaseValueType()->value;
			$fieldJson = $dbw->addQuotes( json_encode( $field ) );
			$createTableFragments[] = "{$dbw->addIdentifierQuotes($field->getFieldName())} {$dbType} COMMENT $fieldJson";
			if ( $field->getIndexed() ) {
				$createTableFragments[] = self::getIndexStatement( $field, $dbw );
			}
		}
		$createTableFragments[] = "PRIMARY KEY ({$dbw->addIdentifierQuotes('_page_id')}, {$dbw->addIdentifierQuotes('_index')})";

		$dbTableName = $dbw->addIdentifierQuotes( $newSchema->getTableName() );
		return "CREATE TABLE $dbTableName (" . implode( ', ', $createTableFragments ) . ');';
	}

	public static function deleteTable( string $bucketName ): void {
		$dbw = self::getDB();
		$bucketName = self::getValidBucketName( $bucketName );
		$tableName = self::getBucketTableName( $bucketName );

		if ( self::countPagesUsingBucket( $bucketName ) > 0 ) {
			$dbw->newDeleteQueryBuilder()
				->table( 'bucket_schemas' )
				->where( [ 'bucket_name' => $bucketName ] )
				->caller( __METHOD__ )
				->execute();
			$dbw->query( "DROP TABLE IF EXISTS $tableName" );
		}
	}

	/**
	 * @return int - The number of pages writing to this bucket
	 */
	public static function countPagesUsingBucket( string $bucketName ): int {
		$dbw = self::getDB();
		$bucketName = self::getValidBucketName( $bucketName );
		return $dbw->newSelectQueryBuilder()
						->table( 'bucket_pages' )
						->lockInShareMode()
						->where( [ 'bucket_name' => $bucketName ] )
						->fetchRowCount();
	}

	private static function getIndexStatement( BucketSchemaField $field, IDatabase $dbw ): string {
		$fieldName = $dbw->addIdentifierQuotes( $field->getFieldName() );
		switch ( $field->getDatabaseValueType() ) {
			case ValueType::Json:
				// Typecasting for repeated fields doesn't give us any advantage
				// return "INDEX $fieldName((CAST($fieldName AS CHAR(512) ARRAY)))"; //TODO Figure out if this larger index is needed or good
				return "INDEX $fieldName((CAST($fieldName AS CHAR(255) ARRAY)))";
			case ValueType::Text:
			case ValueType::Page:
				return "INDEX $fieldName($fieldName(255))";
			default:
				return "INDEX $fieldName($fieldName)";
		}
	}

	public static function getBucketTableName( $bucketName ): string {
		return 'bucket__' . $bucketName;
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
	private int $timestamp = 0;

	function __construct( string $bucketName, array $schema, int $timestamp = 0 ) {
		$this->timestamp = $timestamp;
		if ( $bucketName == '' ) {
			throw new QueryException( wfMessage( 'bucket-empty-bucket-name' ) );
		}
		$this->bucketName = $bucketName;

		foreach ( $schema as $name => $val ) {
			if ( $val instanceof BucketSchemaField ) {
				$this->fields[$val->getFieldName()] = $val;
			} else {
				// Skip the _time field, its not a real field
				if ( $name == '_time' ) {
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
	function getFields(): array {
		return $this->fields;
	}

	function getField( string $fieldName ): BucketSchemaField {
		return $this->fields[$fieldName];
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

	public function jsonSerialize(): mixed {
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

	function __construct( string $fieldName, ValueType $type, bool $indexed, bool $repeated ) {
		$this->fieldName = $fieldName;
		$this->type = $type;
		$this->indexed = $indexed;
		$this->repeated = $repeated;
	}

	function getFieldName(): string {
		return $this->fieldName;
	}

	function getType(): ValueType {
		return $this->type;
	}

	function getIndexed(): bool {
		return $this->indexed;
	}

	function getRepeated(): bool {
		return $this->repeated;
	}

	public function jsonSerialize(): mixed {
		return [
			'type' => $this->getType()->value,
			'index' => $this->getIndexed(),
			'repeated' => $this->getRepeated()
		];
	}

	/**
	 * Example json: {"type":"INTEGER","index":false,"repeated":false}
	 */
	static function fromJson( string $fieldName, string $json ): BucketSchemaField {
		$jsonRow = json_decode( $json, true );
		// This is the old style (Pre-July 2025) of table comment formatting.
		// Example json: {"_page_id":{"type":"INTEGER","index":false,"repeated":false}}
		// This can be removed once old buckets are no longer used (after Bucket is in prod and the dev env has been reset)
		if ( isset( $jsonRow[$fieldName] ) && isset( $jsonRow[$fieldName]['type'] ) ) {
			$jsonRow = $jsonRow[$fieldName];
		}
		// End old style handling
		return new BucketSchemaField( $fieldName, ValueType::from( $jsonRow['type'] ), $jsonRow['index'], $jsonRow['repeated'] );
	}

	/**
	 * The ValueType that this field is stored as in the database.
	 */
	public function getDatabaseValueType(): ValueType {
		if ( $this->getRepeated() ) {
			return ValueType::Json;
		} else {
			// Page is just stored as text in the database
			if ( $this->getType() == ValueType::Page ) {
				return ValueType::Text;
			}
			return $this->getType();
		}
	}

	public function castValueForDatabase( mixed $value ): mixed {
		if ( $value == null ) {
			return null;
		}
		switch ( $this->getDatabaseValueType() ) {
			case ValueType::Text:
			case ValueType::Page:
				if ( $value == '' ) {
					return null;
				} else {
					return $value;
				}
			case ValueType::Double:
				return floatval( $value );
			case ValueType::Integer:
				return intval( $value );
			case ValueType::Boolean:
				return boolval( $value );
			case ValueType::Json:
				if ( !is_array( $value ) ) {
					if ( $value == '' ) {
						return null;
					} else {
						return json_encode( [ $value ] ); // Wrap single values in an array for compatability
					}
				} else {
					if ( count( $value ) > 0 ) {
						return json_encode( LuaLibrary::convertFromLuaTable( $value ) );
					} else {
						return null;
					}
				}
		}
		return null;
	}
}

class BucketException extends LogicException {
	private ?Message $wfMessage = null;

	public function getWfMessage(): Message {
		return $this->wfMessage;
	}

	function __construct( $msg ) {
		$this->wfMessage = $msg;
		parent::__construct( $msg );
	}
}

class SchemaException extends BucketException {
	function __construct( $msg ) {
		parent::__construct( $msg );
	}
}

class QueryException extends BucketException {
	function __construct( $msg ) {
		parent::__construct( $msg );
	}
}
