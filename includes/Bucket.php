<?php

namespace MediaWiki\Extension\Bucket;

use LogicException;
use MediaWiki\MediaWikiServices;
use Message;
use Wikimedia\Rdbms\IDatabase;
use Wikimedia\Rdbms\IMaintainableDatabase;

class Bucket {
	public const EXTENSION_DATA_KEY = 'bucket:puts';
	public const MESSAGE_BUCKET = 'bucket_message';

	private static $db = null;
	private static $specialBucketUser = false;

	private static $dataTypes = [
		'BOOLEAN' => 'BOOLEAN',
		'DOUBLE' => 'DOUBLE',
		'INTEGER' => 'INTEGER',
		'JSON' => 'JSON',
		'TEXT' => 'TEXT',
		'PAGE' => 'TEXT'
	];

	private static $requiredColumns = [
			'_page_id' => [ 'type' => 'INTEGER', 'index' => false, 'repeated' => false ],
			'_index' => [ 'type' => 'INTEGER', 'index' => false, 'repeated' => false ],
			'page_name' => [ 'type' => 'PAGE', 'index' => true, 'repeated' => false ],
			'page_name_sub' => [ 'type' => 'PAGE', 'index' => true, 'repeated' => false ],
	];

	public static function getDB(): IMaintainableDatabase {
		if ( self::$db != null && self::$db->isOpen() ) {
			return self::$db;
		}
		$config = MediaWikiServices::getInstance()->getMainConfig();
		$bucketDBuser = $config->get( 'BucketDBuser' );
		$bucketDBpassword = $config->get( 'BucketDBpassword' );

		$mainDB = self::getMainDB();
		if ( $bucketDBuser == null || $bucketDBpassword == null ) {
			// TODO need to set utf8Mode for this
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

	private static function getBucketDBUser() {
		$config = MediaWikiServices::getInstance()->getMainConfig();
		$dbUser = $config->get( 'BucketDBuser' );
		$dbServer = $config->get( 'BucketDBserver' );
		return "$dbUser@'$dbServer'";
	}

	public static function logMessage( string $bucket, string $property, string $type, string $message, &$logs ) {
		if ( !array_key_exists( self::MESSAGE_BUCKET, $logs ) ) {
			$logs[self::MESSAGE_BUCKET] = [];
		}
		if ( $bucket != '' ) {
			$bucket = 'Bucket:' . $bucket;
		}
		$logs[self::MESSAGE_BUCKET][] = [
			'sub' => '',
			'data' => [
				'bucket' => $bucket,
				'property' => $property,
				'type' => wfMessage( $type ),
				'message' => $message
			]
		];
	}

	/*
	Called when a page is saved containing a bucket.put
	*/
	public static function writePuts( int $pageId, string $titleText, array $puts, bool $writingLogs = false ) {
		$logs = [];
		$dbw = self::getDB();

		$res = $dbw->newSelectQueryBuilder()
				->from( 'bucket_pages' )
				->select( [ '_page_id', 'table_name', 'put_hash' ] )
				->forUpdate()
				->where( [ '_page_id' => $pageId ] )
				->caller( __METHOD__ )
				->fetchResultSet();
		$bucket_hash = [];
		foreach ( $res as $row ) {
			$bucket_hash[ $row->table_name ] = $row->put_hash;
		}

		// Combine existing written bucket list and new written bucket list.
		$relevantBuckets = array_merge( array_keys( $puts ), array_keys( $bucket_hash ) );
		$res = $dbw->newSelectQueryBuilder()
				->from( 'bucket_schemas' )
				->select( [ 'table_name', 'schema_json' ] )
				->lockInShareMode()
				->where( [ 'table_name' => $relevantBuckets ] )
				->caller( __METHOD__ )
				->fetchResultSet();
		$schemas = [];
		foreach ( $res as $row ) {
			$schemas[$row->table_name] = json_decode( $row->schema_json, true );
		}

		foreach ( $puts as $tableName => $tableData ) {
			if ( $tableName == '' ) {
				self::logMessage( $tableName, '', 'bucket-general-error', wfMessage( 'bucket-no-bucket-defined-warning' ), $logs );
				continue;
			}

			$tableNameTmp = self::getValidFieldName( $tableName );
			if ( $tableNameTmp == false ) {
				self::logMessage( $tableName, '', 'bucket-general-warning', wfMessage( 'bucket-invalid-name-warning', $tableName ), $logs );
				continue;
			}
			if ( $tableNameTmp != $tableName ) {
				self::logMessage( $tableName, '', 'bucket-general-warning', wfMessage( 'bucket-capital-name-warning' ), $logs );
			}
			$tableName = $tableNameTmp;

			if ( $tableName == self::MESSAGE_BUCKET && $writingLogs == false ) {
				self::logMessage( $tableName, self::MESSAGE_BUCKET, 'bucket-general-error', wfMessage( 'bucket-cannot-write-to-system-bucket' ), $logs );
				continue;
			}

			if ( !array_key_exists( $tableName, $schemas ) ) {
				self::logMessage( $tableName, '', 'bucket-general-error', wfMessage( 'bucket-no-exist-error' ), $logs );
				continue;
			}

			$tablePuts = [];
			$dbTableName = self::getBucketTableName( $tableName );
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
				if ( !isset( $schemas[$tableName][$fieldName] ) ) {
					self::logMessage( $tableName, $fieldName, 'bucket-general-error', wfMessage( 'bucket-schema-outdated-error' ), $logs );
				} else {
					$fields[$fieldName] = true;
				}
			}
			foreach ( $tableData as $idx => $singleData ) {
				$sub = $singleData['sub'];
				$singleData = $singleData['data'];
				if ( gettype( $singleData ) != 'array' ) {
					self::logMessage( $tableName, '', 'bucket-general-error', wfMessage( 'bucket-put-syntax-error' ), $logs );
					continue;
				}
				foreach ( $singleData as $key => $value ) {
					if ( !isset( $fields[$key] ) || !$fields[$key] ) {
						self::logMessage( $tableName, $key, 'bucket-general-warning', wfMessage( 'bucket-put-key-missing-warning', $key, $tableName ), $logs );
					}
				}
				$singlePut = [];
				foreach ( $fields as $key => $_ ) {
					$value = isset( $singleData[$key] ) ? $singleData[$key] : null;
					$singlePut[$dbw->addIdentifierQuotes( $key )] = self::castToDbType( $value, self::getDbType( $key, $schemas[$tableName][$key] ) );
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
			sort( $schemas[$tableName] );
			$newHash = hash( 'sha256', json_encode( $tablePuts ) . json_encode( $schemas[$tableName] ) );
			if ( isset( $bucket_hash[ $tableName ] ) && $bucket_hash[ $tableName ] == $newHash ) {
				unset( $bucket_hash[ $tableName ] );
				continue;
			}

			// Remove the bucket_hash entry so we can it as a list of removed buckets at the end.
			unset( $bucket_hash[ $tableName ] );

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
				->where( [ '_page_id' => $pageId, 'table_name' => $tableName ] )
				->caller( __METHOD__ )
				->execute();
			$dbw->newInsertQueryBuilder()
				->insert( 'bucket_pages' )
				->rows( [ '_page_id' => $pageId, 'table_name' => $tableName, 'put_hash' => $newHash ] )
				->caller( __METHOD__ )
				->execute();
		}

		if ( !$writingLogs ) {
			// Clean up bucket_pages entries for buckets that are no longer written to on this page.
			$tablesToDelete = array_keys( array_filter( $bucket_hash ) );
			if ( count( $logs ) != 0 ) {
				unset( $tablesToDelete[self::MESSAGE_BUCKET] );
			} else {
				$tablesToDelete[] = self::MESSAGE_BUCKET;
			}

			if ( count( $tablesToDelete ) > 0 ) {
				$dbw->newDeleteQueryBuilder()
					->deleteFrom( 'bucket_pages' )
					->where( [ '_page_id' => $pageId, 'table_name' => $tablesToDelete ] )
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

			if ( count( $logs ) > 0 ) {
				self::writePuts( $pageId, $titleText, $logs, true );
			}
		}
	}

	/**
	 * Called for any page save that doesn't have bucket puts
	 */
	public static function clearOrphanedData( int $pageId ) {
		$dbw = self::getDB();

		// Check if any buckets are storing data for this page
		$res = $dbw->newSelectQueryBuilder()
				->from( 'bucket_pages' )
				->select( [ 'table_name' ] )
				->forUpdate()
				->where( [ '_page_id' => $pageId ] )
				->groupBy( 'table_name' )
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
				$table[] = $row->table_name;
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

	public static function getValidFieldName( ?string $fieldName ) {
		if ( $fieldName != null && preg_match( '/^[a-zA-Z0-9_]+$/', $fieldName ) ) {
			$cleanName = strtolower( trim( $fieldName ) );
			// MySQL has a maximum of 64, lets limit it to 60 in case we need to append to columns for some reason later
			if ( strlen( $cleanName ) <= 60 ) {
				return $cleanName;
			}
		}
		return false;
	}

	public static function getValidBucketName( string $bucketName ) {
		if ( ucfirst( $bucketName ) != ucfirst( strtolower( $bucketName ) ) ) {
			throw new SchemaException( wfMessage( 'bucket-capital-name-error' ) );
		}
		$bucketName = self::getValidFieldName( $bucketName );
		if ( !$bucketName ) {
			throw new SchemaException( wfMessage( 'bucket-invalid-name-warning', $bucketName ) );
		}
		return $bucketName;
	}

	public static function canCreateTable( string $bucketName ) {
		$bucketName = self::getValidBucketName( $bucketName );
		$dbw = self::getDB();
		$schemaExists = $dbw->newSelectQueryBuilder()
			->from( 'bucket_schemas' )
			->where( [ 'table_name' => $bucketName ] )
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

	private static function buildSchemaFromComments( string $dbTableName, IDatabase $dbw ): array {
		$jsonObject = [];
		$res = $dbw->query( "SHOW FULL COLUMNS FROM $dbTableName;", __METHOD__ );

		foreach ( $res as $row => $val ) {
			$jsonObject[] = json_decode( $val->Comment, true );
		}
		return array_merge( ...$jsonObject ); // The ... operator passes each array element as its own parameter.
	}

	public static function createOrModifyTable( string $bucketName, object $jsonSchema, bool $isExistingPage ) {
		$newSchema = array_merge( [], self::$requiredColumns );
		$bucketName = self::getValidBucketName( $bucketName );

		if ( $bucketName == self::MESSAGE_BUCKET ) {
			throw new SchemaException( wfMessage( 'bucket-cannot-create-system-page' ) );
		}

		if ( !$isExistingPage && !self::canCreateTable( $bucketName ) ) {
			throw new SchemaException( wfMessage( 'bucket-already-exist-error' ) );
		}

		if ( empty( (array)$jsonSchema ) ) {
			throw new SchemaException( wfMessage( 'bucket-schema-no-columns-error' ) );
		}

		foreach ( $jsonSchema as $fieldName => $fieldData ) {
			if ( gettype( $fieldName ) !== 'string' ) {
				throw new SchemaException( wfMessage( 'bucket-schema-must-be-strings', $fieldName ) );
			}

			$lcFieldName = self::getValidFieldName( $fieldName );
			if ( !$lcFieldName ) {
				throw new SchemaException( wfMessage( 'bucket-schema-invalid-field-name', $fieldName ) );
			}

			$lcFieldName = strtolower( $fieldName );
			if ( isset( $newSchema[$lcFieldName] ) ) {
				throw new SchemaException( wfMessage( 'bucket-schema-duplicated-field-name', $fieldName ) );
			}

			if ( !isset( self::$dataTypes[$fieldData->type] ) ) {
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

			$newSchema[$lcFieldName] = [ 'type' => $fieldData->type, 'index' => $index, 'repeated' => $repeated ];
		}

		if ( count( $newSchema ) > 64 ) {
			throw new SchemaException( wfMessage( 'bucket-schema-too-many-fields' ) );
		}

		$dbTableName = self::getBucketTableName( $bucketName );
		$dbw = self::getDB();

		$dbw->onTransactionCommitOrIdle( function () use ( $dbw, $dbTableName, $newSchema, $bucketName ) {
			if ( !$dbw->tableExists( $dbTableName, __METHOD__ ) ) {
				// We are a new bucket json
				$statement = self::getCreateTableStatement( $dbTableName, $newSchema, $dbw );
				// Grant perms to the new table
				if ( self::$specialBucketUser ) {
					$bucketDBuser = self::getBucketDBUser();
					$mainDB = self::getMainDB();
					$mainDB->query( $statement );
					$escapedTableName = $mainDB->addIdentifierQuotes( $dbTableName );
					$mainDB->query( "GRANT ALL ON $escapedTableName TO $bucketDBuser;" );
				} else {
					$dbw->query( $statement );
				}
			} else {
				// We are an existing bucket json
				$oldSchema = self::buildSchemaFromComments( $dbTableName, $dbw );
				$statement = self::getAlterTableStatement( $dbTableName, $newSchema, $oldSchema, $dbw );
				$dbw->query( $statement );
			}

			// At this point is is possible that another transaction has changed the table
			//So we start a transaction, read the column comments (which are the schema), and write that to bucket_schemas
			$dbw->begin( __METHOD__ );
			$schemaJson = self::buildSchemaFromComments( $dbTableName, $dbw );
			$schemaJson['_time'] = time(); // Time is only used so that an edit and then a revert will still count as a new schema.
			$schemaJson = json_encode( $schemaJson );
			$dbw->upsert(
				'bucket_schemas',
				[ 'table_name' => $bucketName, 'schema_json' => $schemaJson ],
				'table_name',
				[ 'schema_json' => $schemaJson ]
			);
			$dbw->commit( __METHOD__ );
		}, __METHOD__ );
	}

	private static function getAlterTableStatement( $dbTableName, $newSchema, $oldSchema, IDatabase $dbw ) {
		$alterTableFragments = [];

		$previousColumn = 'page_name_sub';
		foreach ( $newSchema as $fieldName => $fieldData ) {
			$escapedFieldName = $dbw->addIdentifierQuotes( $fieldName );
			$fieldJson = $dbw->addQuotes( json_encode( [ $fieldName => $fieldData ] ) );
			# Handle new columns
			if ( !isset( $oldSchema[$fieldName] ) ) {
				$alterTableFragments[] = "ADD $escapedFieldName " . self::getDbType( $fieldName, $fieldData ) . " COMMENT $fieldJson AFTER {$dbw->addIdentifierQuotes($previousColumn)}";
				if ( $fieldData['index'] ) {
					$alterTableFragments[] = 'ADD ' . self::getIndexStatement( $fieldName, $fieldData, $dbw );
				}
			# Handle removing index
			} elseif ( $oldSchema[$fieldName]['index'] === true && $fieldData['index'] === false ) {
				$alterTableFragments[] = "MODIFY $escapedFieldName " . self::getDbType( $fieldName, $fieldData ) . " COMMENT $fieldJson"; // Acts as a no-op except to set the comment
				$alterTableFragments[] = "DROP INDEX $escapedFieldName";
			} else {
				# Handle type changes
				$oldDbType = self::getDbType( $fieldName, $oldSchema[$fieldName] );
				$newDbType = self::getDbType( $fieldName, $fieldData );
				if ( $oldDbType !== $newDbType ) { # Always drop and then re-add the column for type changes.
					if ( $oldSchema[$fieldName]['index'] ) {
						$alterTableFragments[] = "DROP INDEX $escapedFieldName";
					}
					$alterTableFragments[] = "DROP $escapedFieldName";
					$alterTableFragments[] = "ADD $escapedFieldName " . self::getDbType( $fieldName, $fieldData ) . " COMMENT $fieldJson AFTER {$dbw->addIdentifierQuotes($previousColumn)}";
					if ( $fieldData['index'] ) {
						$alterTableFragments[] = 'ADD ' . self::getIndexStatement( $fieldName, $fieldData, $dbw );
					}
				# Handle adding index without type change
				} elseif ( ( $oldSchema[$fieldName]['index'] === false && $fieldData['index'] === true ) ) {
					$alterTableFragments[] = "MODIFY $escapedFieldName " . self::getDbType( $fieldName, $fieldData ) . " COMMENT $fieldJson"; // Acts as a no-op except to set the comment
					$alterTableFragments[] = 'ADD ' . self::getIndexStatement( $fieldName, $fieldData, $dbw );
				# Handle changing between types that don't actually change the DB type
				} elseif ( ( $oldSchema[$fieldName]['type'] != $newSchema[$fieldName]['type'] ) ) {
					$alterTableFragments[] = "MODIFY $escapedFieldName " . self::getDbType( $fieldName, $fieldData ) . " COMMENT $fieldJson"; // Acts as a no-op except to set the comment
				}
			}
			unset( $oldSchema[$fieldName] );
			$previousColumn = $fieldName;
		}
		// Drop unused columns
		foreach ( $oldSchema as $deletedColumn => $val ) {
			$escapedDeletedColumn = $dbw->addIdentifierQuotes( $deletedColumn );
			if ( $oldSchema[$deletedColumn]['repeated'] === true ) {
				$alterTableFragments[] = "DROP INDEX $escapedDeletedColumn"; // We must explicitly drop indexes for repeated fields
			}
			$alterTableFragments[] = "DROP $escapedDeletedColumn";
		}

		return "ALTER TABLE $dbTableName " . implode( ', ', $alterTableFragments ) . ';';
	}

	private static function getCreateTableStatement( $dbTableName, $newSchema, IDatabase $dbw ) {
		$createTableFragments = [];

		foreach ( $newSchema as $fieldName => $fieldData ) {
			$dbType = self::getDbType( $fieldName, $fieldData );
			$fieldJson = $dbw->addQuotes( json_encode( [ $fieldName => $fieldData ] ) );
			$createTableFragments[] = "{$dbw->addIdentifierQuotes($fieldName)} {$dbType} COMMENT $fieldJson";
			if ( $fieldData['index'] ) {
				$createTableFragments[] = self::getIndexStatement( $fieldName, $fieldData, $dbw );
			}
		}
		$createTableFragments[] = "PRIMARY KEY ({$dbw->addIdentifierQuotes('_page_id')}, {$dbw->addIdentifierQuotes('_index')})";

		$dbTableName = $dbw->addIdentifierQuotes( $dbTableName );
		return "CREATE TABLE $dbTableName (" . implode( ', ', $createTableFragments ) . ');';
	}

	public static function deleteTable( $bucketName ) {
		$dbw = self::getDB();
		$bucketName = self::getValidBucketName( $bucketName );
		$tableName = self::getBucketTableName( $bucketName );

		if ( self::canDeleteBucketPage( $bucketName ) ) {
			$dbw->newDeleteQueryBuilder()
				->table( 'bucket_schemas' )
				->where( [ 'table_name' => $bucketName ] )
				->caller( __METHOD__ )
				->execute();
			$dbw->query( "DROP TABLE IF EXISTS $tableName" );
		}
	}

	public static function canDeleteBucketPage( $bucketName ) {
		$dbw = self::getDB();
		$bucketName = self::getValidBucketName( $bucketName );
		$putCount = $dbw->newSelectQueryBuilder()
						->table( 'bucket_pages' )
						->lockInShareMode()
						->where( [ 'table_name' => $bucketName ] )
						->fetchRowCount();
		if ( $putCount > 0 ) {
			return false;
		}
		return true;
	}

	public static function getDbType( string $fieldName, ?array $fieldData ): string {
		if ( isset( self::$requiredColumns[$fieldName] ) ) {
			return self::$dataTypes[self::$requiredColumns[$fieldName]['type']];
		} else {
			if ( isset( $fieldData['repeated'] ) && strlen( $fieldData['repeated'] ) > 0 ) {
				return 'JSON';
			} else {
				return self::$dataTypes[self::$dataTypes[$fieldData['type']]];
			}
		}
	}

	/**
	 * @param string $fieldName
	 * @param array $fieldData
	 * @return string
	 */
	private static function getIndexStatement( string $fieldName, array $fieldData, IDatabase $dbw ) {
		$unescapedFieldName = $fieldName;
		$fieldName = $dbw->addIdentifierQuotes( $fieldName );
		switch ( self::getDbType( $unescapedFieldName, $fieldData ) ) {
			case 'JSON':
				// Typecasting for repeated fields doesn't give us any advantage
				return "INDEX $fieldName((CAST($fieldName AS CHAR(255) ARRAY)))";
			case 'TEXT':
			case 'PAGE':
				return "INDEX $fieldName($fieldName(255))";
			default:
				return "INDEX $fieldName($fieldName)";
		}
	}

	public static function castToDbType( $value, $type ) {
		if ( $type === 'TEXT' || $type === 'PAGE' ) {
			if ( $value == '' ) {
				return null;
			} else {
				return $value;
			}
		} elseif ( $type === 'DOUBLE' ) {
			return floatval( $value );
		} elseif ( $type === 'INTEGER' ) {
			return intval( $value );
		} elseif ( $type === 'BOOLEAN' ) {
			return boolval( $value );
		} elseif ( $type === 'JSON' ) {
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
	}

	public static function getBucketTableName( $bucketName ): string {
		return 'bucket__' . $bucketName;
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
