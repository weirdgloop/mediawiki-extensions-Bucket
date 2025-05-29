<?php

namespace MediaWiki\Extension\Bucket;

use LogicException;
use MediaWiki\MediaWikiServices;
use Message;
use Wikimedia\Rdbms\IDatabase;
use Wikimedia\Rdbms\IMaintainableDatabase;

class Bucket {
	public const EXTENSION_DATA_KEY = 'bucket:puts';
	public const EXTENSION_PROPERTY_KEY = 'bucketputs';
	public const MAX_LIMIT = 5000;
	public const DEFAULT_LIMIT = 500;
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

	private static $allSchemas = [];
	private static $WHERE_OPS = [
		'='  => true,
		'!=' => true,
		'>=' => true,
		'<=' => true,
		'>'  => true,
		'<'  => true,
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
			self::$db = $mainDB;
			self::$specialBucketUser = false;
			return self::$db;
		}

		$params = [
			'host' => $mainDB->getServer(),
			'user' => $bucketDBuser,
			'password' => $bucketDBpassword,
			'dbname' => $mainDB->getDBname()
		];

		self::$db = MediaWikiServices::getInstance()->getDatabaseFactory()->create( $mainDB->getType(), $params );
		self::$specialBucketUser = true;
		return self::$db;
	}

	private static function getMainDB(): IMaintainableDatabase {
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
					# TODO JSON relies on forcing utf8 transmission in DatabaseMySQL.php line 829
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
		if ( $fieldName != null && preg_match( '/^[a-zA-Z0-9_ ]+$/', $fieldName ) ) {
			$cleanName = str_replace( ' ', '_', strtolower( trim( $fieldName ) ) );
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

	public static function createOrModifyTable( string $bucketName, object $jsonSchema, int $parentId ) {
		$newSchema = array_merge( [], self::$requiredColumns );
		$bucketName = self::getValidBucketName( $bucketName );

		if ( $bucketName == self::MESSAGE_BUCKET ) {
			throw new SchemaException( wfMessage( 'bucket-cannot-create-system-page' ) );
		}

		if ( $parentId == 0 && !self::canCreateTable( $bucketName ) ) {
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

		$dbw->onTransactionCommitOrIdle( function () use ( $dbw, $dbTableName, $newSchema, $parentId, $bucketName ) {
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

		unset( $oldSchema['_parent_rev_id'] ); // _parent_rev_id is not a column, its just metadata
		foreach ( $newSchema as $fieldName => $fieldData ) {
			$escapedFieldName = $dbw->addIdentifierQuotes( $fieldName );
			$fieldJson = $dbw->addQuotes( json_encode( [ $fieldName => $fieldData ] ) );
			# Handle new columns
			if ( !isset( $oldSchema[$fieldName] ) ) {
				$alterTableFragments[] = "ADD $escapedFieldName " . self::getDbType( $fieldName, $fieldData ) . " COMMENT $fieldJson";
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
				if ( $oldDbType !== $newDbType ) {
					$needNewIndex = false;
					if ( $oldSchema[$fieldName]['repeated'] || $fieldData['repeated']
						|| strpos( self::getIndexStatement( $fieldName, $oldSchema[$fieldName], $dbw ), '(' ) != strpos( self::getIndexStatement( $fieldName, $fieldData, $dbw ), '(' ) ) {
						# We cannot MODIFY from a column that doesn't need key length to a column that does need key length
						if ( $oldSchema[$fieldName]['index'] ) {
							$alterTableFragments[] = "DROP INDEX $escapedFieldName"; # Repeated types cannot reuse the existing index
						}
						$needNewIndex = true;
					}
					if ( $oldDbType == 'TEXT' && $newDbType == 'JSON' ) { # Update string types to be valid JSON
						$dbw->onTransactionCommitOrIdle( static function () use ( $dbw, $dbTableName, $escapedFieldName ) {
							$dbw->query( "UPDATE $dbTableName SET $escapedFieldName = JSON_ARRAY($escapedFieldName) WHERE NOT JSON_VALID($escapedFieldName) AND _page_id >= 0;" );
						}, __METHOD__ );
					}
					$alterTableFragments[] = "MODIFY $escapedFieldName " . self::getDbType( $fieldName, $fieldData ) . " COMMENT $fieldJson";
					if ( $fieldData['index'] && $needNewIndex ) {
						$alterTableFragments[] = 'ADD ' . self::getIndexStatement( $fieldName, $fieldData, $dbw );
					}
				} else {
					# Handle adding index without type change
					if ( ( $oldSchema[$fieldName]['index'] === false && $fieldData['index'] === true ) ) {
						$alterTableFragments[] = "MODIFY $escapedFieldName " . self::getDbType( $fieldName, $fieldData ) . " COMMENT $fieldJson"; // Acts as a no-op except to set the comment
						$alterTableFragments[] = 'ADD ' . self::getIndexStatement( $fieldName, $fieldData, $dbw );
					}
				}
			}
			unset( $oldSchema[$fieldName] );
		}
		// Drop unused columns
		foreach ( $oldSchema as $deletedColumn => $val ) {
			$alterTableFragments[] = "DROP {$dbw->addIdentifierQuotes($deletedColumn)}";
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

	public static function isBucketWithPuts( $cleanBucketName, IDatabase $dbw ) {
		return $dbw->newSelectQueryBuilder()
			->table( 'bucket_pages' )
			->lockInShareMode()
			->where( [ 'table_name' => $cleanBucketName ] )
			->fetchRowCount() !== 0;
	}

	private static function getDbType( string $fieldName, ?array $fieldData ): string {
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
				$fieldData['repeated'] = false;
				$subType = self::getDbType( $unescapedFieldName, $fieldData );
				switch ( $subType ) {
					case 'TEXT':
						$subType = 'CHAR(255)';
						break;
					case 'INTEGER':
						$subType = 'DECIMAL';
						break;
					case 'DOUBLE': // CAST doesn't have a double type
						$subType = 'CHAR(255)';
						break;
					case 'BOOLEAN':
						$subType = 'CHAR(255)'; // CAST doesn't have a boolean option
						break;
				}
				return "INDEX $fieldName((CAST($fieldName AS $subType ARRAY)))";
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
				// Remove empty strings
				$value = array_filter( $value, static function ( $v ) {
					return $v != '';
				} );
				if ( count( $value ) > 0 ) {
					return json_encode( LuaLibrary::convertFromLuaTable( $value ) );
				} else {
					return null;
				}
			}
		}
	}

	public static function cast( $value, $fieldData ) {
		$type = $fieldData['type'];
		if ( $fieldData['repeated'] ) {
			$ret = [];
			$fieldData['repeated'] = false;
			if ( $value == null ) {
				$value = '';
			}
			$jsonData = json_decode( $value, true );
			if ( !is_array( $jsonData ) ) { // If we are in a repeated field but only holding a scalar, make it an array anyway.
				$jsonData = [ $jsonData ];
			}
			foreach ( $jsonData as $subVal ) {
				$ret[] = self::cast( $subVal, $fieldData );
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

	public static function sanitizeColumnName( $column, $fieldNamesToTables, $schemas, IDatabase $dbw, $tableName = null ) {
		if ( !is_string( $column ) ) {
			throw new QueryException( wfMessage( 'bucket-query-column-interpret-error', $column ) );
		}
		// Category column names are specially handled
		if ( self::isCategory( $column ) ) {
			$tableName = 'category';
			$columnName = explode( ':', $column )[1];
			$bucketName = self::getBucketTableName( $tableName );
			return [
				'fullName' => $dbw->addIdentifierQuotes( $bucketName ) . '.' . $dbw->addIdentifierQuotes( $columnName ),
				'tableName' => $tableName,
				'columnName' => $columnName,
				'schema' => [
					'type' => 'BOOLEAN',
					'index' => false,
					'repeated' => false
				]
			];
		}
		$parts = explode( '.', $column );
		if ( $column === '' || count( $parts ) > 2 ) {
			throw new QueryException( wfMessage( 'bucket-query-column-name-invalid', $column ) );
		}
		$columnNameTemp = end( $parts );
		$columnName = self::getValidFieldName( $columnNameTemp );
		if ( !$columnName ) {
			throw new QueryException( wfMessage( 'bucket-query-column-name-invalid', $columnNameTemp ) );
		}
		if ( count( $parts ) === 1 ) {
			if ( !isset( $fieldNamesToTables[$columnName] ) ) {
				throw new QueryException( wfMessage( 'bucket-query-column-not-found', $columnName ) );
			}
			if ( $tableName === null ) {
				$tableOptions = $fieldNamesToTables[$columnName];
				if ( count( $tableOptions ) > 1 ) {
					throw new QueryException( wfMessage( 'bucket-query-column-ambiguous', $columnName ) );
				}
				$tableName = array_keys( $tableOptions )[0];
			}
		} elseif ( count( $parts ) === 2 ) {
			$columnTableName = self::getValidFieldName( $parts[0] );
			if ( !$columnTableName ) {
				throw new QueryException( wfMessage( 'bucket-invalid-name-warning', $parts[0] ) );
			}
			if ( $tableName !== null && $columnTableName !== $tableName ) {
				throw new QueryException( wfMessage( 'bucket-query-bucket-invalid', $parts[0] ) );
			}
			$tableName = $columnTableName;
		}
		if ( !isset( $schemas[$tableName] ) ) {
			throw new QueryException( wfMessage( 'bucket-query-bucket-not-found', $tableName ) );
		}
		if ( !isset( $schemas[$tableName][$columnName] ) ) {
			throw new QueryException( wfMessage( 'bucket-query-column-not-found-in-bucket', $columnName, $tableName ) );
		}
		$bucketName = self::getBucketTableName( $tableName );
		return [
			'fullName' => $dbw->addIdentifierQuotes( $bucketName ) . '.' . $dbw->addIdentifierQuotes( $columnName ),
			'tableName' => $tableName,
			'columnName' => $columnName,
			'schema' => $schemas[$tableName][$columnName]
		];
	}

	private static function sanitizeValue( $value, IDatabase $dbw ) {
		if ( is_numeric( $value ) ) {
			if ( is_int( $value ) ) {
				return intval( $value );
			}
			if ( is_float( $value ) ) { // Float and double
				return floatval( $value );
			}
			throw new QueryException( wfMessage( 'bucket-query-cast-fail', $value ) );
		}
		if ( is_bool( $value ) ) {
			return boolval( $value );
		}
		if ( is_string( $value ) ) {
			return $dbw->addQuotes( strval( $value ) );
		}
		throw new QueryException( wfMessage( 'bucket-query-cast-fail', $value ) );
	}

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

	public static function isCategory( $columnName ) {
		return substr( strtolower( trim( $columnName ) ), 0, 9 ) == 'category:';
	}

	/**
	 *  $condition is an array of members:
	 * 		operands -> Array of $conditions
	 * 		(optional)op -> AND | OR | NOT
	 * 		unnamed -> scalar value or array of scalar values
	 */
	public static function getWhereCondition( $condition, $fieldNamesToTables, $schemas, IDatabase $dbw, &$categoryJoins ) {
		if ( self::isOrAnd( $condition ) ) {
			if ( empty( $condition['operands'] ) ) {
				throw new QueryException( wfMessage( 'bucket-query-where-missing-cond', json_encode( $condition ) ) );
			}
			$children = [];
			foreach ( $condition['operands'] as $key => $operand ) {
				if ( $key != 'op' ) { // the key 'op' will never be a valid condition on its own.
					if ( !isset( $operand['op'] ) && isset( $condition['op'] ) && isset( $operand[0] ) && is_array( $operand[0] ) && count( $operand[0] ) > 0 ) {
						$operand['op'] = $condition['op']; // Set child op to parent
					}
					$children[] = self::getWhereCondition( $operand, $fieldNamesToTables, $schemas, $dbw, $categoryJoins );
				}
			}
			if ( $condition['op'] == 'OR' ) {
				return $dbw->makeList( $children, IDatabase::LIST_OR );
			} else {
				return $dbw->makeList( $children, IDatabase::LIST_AND );
			}
		} elseif ( self::isNot( $condition ) ) {
			$child = self::getWhereCondition( $condition['operand'], $fieldNamesToTables, $schemas, $dbw, $categoryJoins );
			return "(NOT $child)";
		} elseif ( is_array( $condition ) && isset( $condition[0] ) && is_array( $condition[0] ) ) {
			// .where{{"a", ">", 0}, {"b", "=", "5"}})
			return self::getWhereCondition( [ 'op' => isset( $condition[ 'op' ] ) ? $condition[ 'op' ] : 'AND', 'operands' => $condition ], $fieldNamesToTables, $schemas, $dbw, $categoryJoins );
		} elseif ( is_array( $condition ) && !empty( $condition ) && !isset( $condition[0] ) ) {
			// .where({a = 1, b = 2})
			$operands = [];
			foreach ( $condition as $key => $value ) {
				$operands[] = [ $key, '=', $value ];
			}
			return self::getWhereCondition( [ 'op' => 'AND', 'operands' => $operands ], $fieldNamesToTables, $schemas, $dbw, $categoryJoins );
		} elseif ( is_array( $condition ) && isset( $condition[0] ) && isset( $condition[1] ) ) {
			if ( count( $condition ) === 2 ) {
				$condition = [ $condition[0], '=', $condition[1] ];
			}
			$columnNameData = self::sanitizeColumnName( $condition[0], $fieldNamesToTables, $schemas, $dbw );
			if ( !isset( self::$WHERE_OPS[$condition[1]] ) ) {
				throw new QueryException( wfMessage( 'bucket-query-where-invalid-op', $condition[1] ) );
			}
			$op = $condition[1];
			$valueUnescaped = $condition[2]; // Use this only to pass value to functions that will escape it themselves.
			$value = self::sanitizeValue( $valueUnescaped, $dbw );

			$columnName = $columnNameData['fullName'];
			$columnData = $fieldNamesToTables[$columnNameData['columnName']][$columnNameData['tableName']];
			if ( $value == '&&NULL&&' ) {
				if ( $op == '!=' ) {
					return "($columnName IS NOT NULL)";
				}
				return "($columnName IS NULL)";
			} elseif ( $columnData['repeated'] == true ) {
				if ( $op == '=' ) {
					return "$value MEMBER OF($columnName)";
				}
				if ( $op == '!=' ) {
					return "NOT $value MEMBER OF($columnName)";
				}
				// > < >= <=
				//TODO this is very expensive
				$columnData['repeated'] = false; // Set repeated to false to get the underlying type
				$dbType = self::getDbType( $columnNameData['fullName'], $columnData );
				// We have to reverse the direction of < > <= >= because SQL requires this condition to be $value $op $column
				//and user input is in order $column $op $value
				$op = strtr( $op, [ '<' => '>', '>' => '<' ] );
				return "($value $op ANY(SELECT json_col FROM JSON_TABLE($columnName, '$[*]' COLUMNS(json_col $dbType PATH '$')) AS json_tab))";
			} else {
				if ( in_array( $op, [ '>', '>=', '<', '<=' ] ) ) {
					return $dbw->buildComparison( $op, [ $columnName => $valueUnescaped ] );
				} elseif ( $op == '=' ) {
					return $dbw->makeList( [ $columnName => $valueUnescaped ], IDatabase::LIST_AND );
				} elseif ( $op == '!=' ) {
					return "($columnName $op $value)";
				} else {
					throw new QueryException( wfMessage( 'bucket-query-where-confused', json_encode( $condition ) ) );
				}
			}
		} elseif ( is_string( $condition ) && self::isCategory( $condition ) || ( is_array( $condition ) && self::isCategory( $condition[0] ) ) ) {
			if ( is_array( $condition ) ) {
				$condition = $condition[0];
			}
			$categoryName = explode( ':', $condition )[1];
			$categoryJoins[$categoryName] = $condition;
			return "({$dbw->addIdentifierQuotes($condition)}.cl_to IS NOT NULL)";
		}
		throw new QueryException( wfMessage( 'bucket-query-where-confused', json_encode( $condition ) ) );
	}

	public static function getBucketTableName( $bucketName ): string {
		return 'bucket__' . $bucketName;
	}

	public static function runSelect( $data ) {
		$SELECTS = [];
		$LEFT_JOINS = [];
		$TABLES = [];
		$WHERES = [];
		$OPTIONS = [];
		// check to see if any duplicates
		$tableNames = [];
		$categoryJoins = [];

		if ( !isset( $data['tableName'] ) ) {
			throw new QueryException( wfMessage( 'bucket-empty-table-name' ) );
		}

		$primaryTableName = self::getValidFieldName( $data['tableName'] );
		if ( !$primaryTableName ) {
			throw new QueryException( wfMessage( 'bucket-invalid-name-warning', $data['tableName'] ) );
		}
		$tableNames[ $primaryTableName ] = true;
		$TABLES[self::getBucketTableName( $primaryTableName )] = self::getBucketTableName( $primaryTableName );

		foreach ( $data['joins'] as $join ) {
			$tableName = self::getValidFieldName( $join['tableName'] );
			if ( !$tableName ) {
				throw new QueryException( wfMessage( 'bucket-invalid-name-warning', $join['tableName'] ) );
			}
			if ( isset( $tableNames[$tableName] ) ) {
				throw new QueryException( wfMessage( 'bucket-select-duplicate-join', $tableName ) );
			}
			$tableNames[$tableName] = true;
			$TABLES[self::getBucketTableName( $tableName )] = self::getBucketTableName( $tableName );
			$join['tableName'] = $tableName;
		}

		$tableNamesList = array_keys( $tableNames );
		foreach ( $tableNames as $tableName => $val ) {
			if ( isset( self::$allSchemas[$tableName] ) && self::$allSchemas[$tableName] ) {
				unset( $tableNames[$tableName] );
			}
		}

		$dbw = self::getDB();
		$missingTableNames = array_keys( $tableNames );
		if ( !empty( $missingTableNames ) ) {
			$res = $dbw->newSelectQueryBuilder()
				->from( 'bucket_schemas' )
				->select( [ 'table_name', 'schema_json' ] )
				->lockInShareMode()
				->where( [ 'table_name' => $missingTableNames ] )
				->caller( __METHOD__ )
				->fetchResultSet();

			$schemas = [];
			foreach ( $res as $row ) {
				self::$allSchemas[$row->table_name] = json_decode( $row->schema_json, true );
			}
		}
		foreach ( $tableNamesList as $tableName ) {
			if ( !array_key_exists( $tableName, self::$allSchemas ) || !self::$allSchemas[$tableName] ) {
				throw new QueryException( wfMessage( 'bucket-no-exist', $tableName ) );
			}
		}

		$schemas = [];
		foreach ( $tableNamesList as $tableName ) {
			$schemas[$tableName] = self::$allSchemas[$tableName];
		}

		$fieldNamesToTables = [];
		foreach ( $schemas as $tableName => $schema ) {
			foreach ( $schema as $fieldName => $fieldData ) {
				if ( substr( $fieldName, 0, 1 ) !== '_' ) {
					if ( !isset( $fieldNamesToTables[$fieldName] ) ) {
						$fieldNamesToTables[$fieldName] = [];
					}
					$fieldNamesToTables[$fieldName][$tableName] = $fieldData;
				}
			}
		}

		$ungroupedColumns = [];
		foreach ( $data['selects'] as $selectColumn ) {
			if ( self::isCategory( $selectColumn ) ) {
				$SELECTS[$selectColumn] = "{$dbw->addIdentifierQuotes($selectColumn)}.cl_to IS NOT NULL";
				$categoryName = explode( ':', $selectColumn )[1];
				$categoryJoins[$categoryName] = $selectColumn;
				continue;
			} else {
				$selectTableName = null;
				// If we don't have a period then we must be the primary column.
				if ( count( explode( '.', $selectColumn ) ) == 1 ) {
					$selectTableName = $primaryTableName;
				}
				$colData = self::sanitizeColumnName( $selectColumn, $fieldNamesToTables, $schemas, $dbw, $selectTableName );

				if ( $colData['tableName'] != $primaryTableName ) {
					$SELECTS[$colData['tableName'] . '.' . $colData['columnName']] = 'JSON_ARRAY(' . $colData['fullName'] . ')';
				} else {
					$SELECTS[$colData['columnName']] = $colData['fullName'];
				}
			}
			$ungroupedColumns[$colData['fullName']] = true;
		}

		if ( !empty( $data['wheres']['operands'] ) ) {
			$WHERES[] = self::getWhereCondition( $data['wheres'], $fieldNamesToTables, $schemas, $dbw, $categoryJoins );
		}

		if ( !empty( $categoryJoins ) ) {

			foreach ( $categoryJoins as $categoryName => $alias ) {
				$TABLES[$alias] = 'categorylinks';
				$bucketName = self::getBucketTableName( $primaryTableName );
				$LEFT_JOINS[$alias] = [
					"{$dbw->addIdentifierQuotes($alias)}.cl_from = {$dbw->addIdentifierQuotes($bucketName)}._page_id", // Must be all in one string to avoid the table name being treated as a string value.
					"{$dbw->addIdentifierQuotes($alias)}.cl_to" => str_replace( ' ', '_', $categoryName )
				];
			}
		}

		foreach ( $data['joins'] as $join ) {
			if ( !is_array( $join['cond'] ) || !count( $join['cond'] ) == 2 ) {
				throw new QueryException( wfMessage( 'bucket-query-invalid-join', json_encode( $join ) ) );
			}
			$leftField = self::sanitizeColumnName( $join['cond'][0], $fieldNamesToTables, $schemas, $dbw );
			$isLeftRepeated = $leftField['schema']['repeated'];
			$rightField = self::sanitizeColumnName( $join['cond'][1], $fieldNamesToTables, $schemas, $dbw );
			$isRightRepeated = $rightField['schema']['repeated'];

			if ( $isLeftRepeated && $isRightRepeated ) {
				throw new QueryException( wfMessage( 'bucket-invalid-join-two-repeated', $leftField['fullName'], $rightField['fullName'] ) );
			}

			if ( $isLeftRepeated || $isRightRepeated ) {
				// Make the left field the repeated one just for consistency.
				if ( $isRightRepeated ) {
					$tmp = $leftField;
					$isTmp = $isLeftRepeated;
					$leftField = $rightField;
					$isLeftRepeated = $isRightRepeated;
					$rightField = $tmp;
					$isRightRepeated = $isTmp;
				}

				$LEFT_JOINS[self::getBucketTableName( $join['tableName'] )] = [
					"{$rightField['fullName']} MEMBER OF({$leftField['fullName']})"
				];
			} else {
				$LEFT_JOINS[self::getBucketTableName( $join['tableName'] )] = [
					"{$leftField['fullName']} = {$rightField['fullName']}"
				];
			}
		}

		$OPTIONS['GROUP BY'] = array_keys( $ungroupedColumns );

		$OPTIONS['LIMIT'] = self::DEFAULT_LIMIT;
		if ( isset( $data['limit'] ) && is_int( $data['limit'] ) && $data['limit'] >= 0 ) {
			$OPTIONS['LIMIT'] = min( $data['limit'], self::MAX_LIMIT );
		}

		$OPTIONS['OFFSET'] = 0;
		if ( isset( $data['offset'] ) && is_int( $data['offset'] ) && $data['offset'] >= 0 ) {
			$OPTIONS['OFFSET'] = $data['offset'];
		}

		$rows = [];
		$tmp = $dbw->newSelectQueryBuilder()
			->from( self::getBucketTableName( $primaryTableName ) )
			->select( $SELECTS )
			->where( $WHERES )
			->options( $OPTIONS )
			->caller( __METHOD__ )
			->setMaxExecutionTime( 500 );
		foreach ( $LEFT_JOINS as $alias => $conds ) {
			$tmp->leftJoin( $TABLES[$alias], $alias, $conds );
		}
		if ( isset( $data['orderBy'] ) ) {
			$orderName = self::sanitizeColumnName( $data['orderBy']['fieldName'], $fieldNamesToTables, $schemas, $dbw )['fullName'];
			if ( $orderName != false ) {
				$tmp->orderBy( $orderName, $data['orderBy']['direction'] );
			}
		}
		file_put_contents( MW_INSTALL_PATH . '/cook.txt', 'Query: ' . print_r( $tmp->getSQL(), true ) . "\n", FILE_APPEND );
		$res = $tmp->fetchResultSet();
		foreach ( $res as $row ) {
			$row = (array)$row;
			foreach ( $row as $columnName => $value ) {
				$defaultTableName = null;
				// If we don't have a period in the column name it must be a primary table column.
				if ( count( explode( '.', $columnName ) ) == 1 ) {
					$defaultTableName = $primaryTableName;
				}
				$schema = self::sanitizeColumnName( $columnName, $fieldNamesToTables, $schemas, $dbw, $defaultTableName )['schema'];
				$row[$columnName] = self::cast( $value, $schema );
			}
			$rows[] = $row;
		}
		return $rows;
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
		file_put_contents( MW_INSTALL_PATH . '/cook.txt', 'SCHEMA EXCEPTION ' . print_r( $msg, true ) . "\n", FILE_APPEND );
		parent::__construct( $msg );
	}
}

class QueryException extends BucketException {
	function __construct( $msg ) {
		file_put_contents( MW_INSTALL_PATH . '/cook.txt', 'QUERY EXCEPTION ' . print_r( $msg, true ) . "\n", FILE_APPEND );
		parent::__construct( $msg );
	}
}
