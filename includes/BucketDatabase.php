<?PHP

namespace MediaWiki\Extension\Bucket;

use MediaWiki\MediaWikiServices;
use Wikimedia\Rdbms\IDatabase;
use Wikimedia\Rdbms\IMaintainableDatabase;

class BucketDatabase {
	private static IMaintainableDatabase $db;

	public static function getDB(): IMaintainableDatabase {
		if ( isset( self::$db ) && self::$db->isOpen() ) {
			return self::$db;
		}
		$config = MediaWikiServices::getInstance()->getMainConfig();
		$bucketDBuser = $config->get( 'BucketDBuser' );
		$bucketDBpassword = $config->get( 'BucketDBpassword' );

		$mainDB = MediaWikiServices::getInstance()->getDBLoadBalancer()->getConnection( DB_PRIMARY );
		$params = [
			'host' => $mainDB->getServer(),
			'user' => $bucketDBuser,
			'password' => $bucketDBpassword,
			'dbname' => $mainDB->getDBname(),
			'utf8Mode' => true,
			'defaultMaxExecutionTimeForQueries' => 500 // WeirdGloop specific tweak
		];

		self::$db = MediaWikiServices::getInstance()->getDatabaseFactory()->create( $mainDB->getType(), $params );
		return self::$db;
	}

	private static function getBucketDBUser(): string {
		$config = MediaWikiServices::getInstance()->getMainConfig();
		$dbUser = $config->get( 'BucketDBuser' );
		$dbServer = $config->get( 'BucketDBhostname' );
		return "$dbUser@'$dbServer'";
	}

	public static function canCreateTable( string $bucketName ): bool {
		$bucketName = Bucket::getValidBucketName( $bucketName );
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
		$bucketName = Bucket::getValidBucketName( $bucketName );
		$newSchema = [
			'_page_id' => new BucketSchemaField( '_page_id', ValueType::Integer, false, false ),
			'_index' => new BucketSchemaField( '_index', ValueType::Integer, false, false ),
			'page_name' => new BucketSchemaField( 'page_name', ValueType::Page, true, false ),
			'page_name_sub' => new BucketSchemaField( 'page_name_sub', ValueType::Page, true, false )
		];

		if ( $bucketName == Bucket::MESSAGE_BUCKET ) {
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

			$lcFieldName = Bucket::getValidFieldName( $fieldName );

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
				$bucketDBuser = self::getBucketDBUser();
				$escapedTableName = $bucketSchema->getSafe( $dbw );
				// Note: The main database connection is only used to grant access to the new table.
				MediaWikiServices::getInstance()->getDBLoadBalancer()
					->getConnection( DB_PRIMARY )->query( "GRANT ALL ON $escapedTableName TO $bucketDBuser;" );
				$dbw->query( $statement );
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
		$bucketName = Bucket::getValidBucketName( $bucketName );
		$tableName = self::getBucketTableName( $bucketName );

		if ( Bucket::countPagesUsingBucket( $bucketName ) > 0 ) {
			$dbw->newDeleteQueryBuilder()
				->table( 'bucket_schemas' )
				->where( [ 'bucket_name' => $bucketName ] )
				->caller( __METHOD__ )
				->execute();
			$dbw->query( "DROP TABLE IF EXISTS $tableName" );
		}
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
