<?php

namespace MediaWiki\Extension\Bucket;

use MediaWiki\Config\ConfigException;
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

		if ( $bucketDBuser === null || $bucketDBpassword === null ) {
			throw new ConfigException( 'BucketDBuser and BucketDBpassword are required config options' );
		}

		$mainDB = MediaWikiServices::getInstance()->getDBLoadBalancer()->getConnection( DB_PRIMARY );
		$params = [
			'host' => $mainDB->getServer(),
			'user' => $bucketDBuser,
			'password' => $bucketDBpassword,
			'dbname' => $mainDB->getDBname(),
			'utf8Mode' => true,
			// Weird Gloop specific tweak
			'defaultMaxExecutionTimeForQueries' => 500
		];

		self::$db = MediaWikiServices::getInstance()->getDatabaseFactory()->create( $mainDB->getType(), $params );

		// MySQL 8.0.17 or higher is required for the implementation of repeated fields.
		if ( self::$db->getType() !== 'mysql' || version_compare( self::$db->getServerVersion(), '8.0.17', '<' ) ) {
			throw new ConfigException( 'Bucket requires MySQL 8.0.17 or higher' );
		}
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
		$dbTableName = $dbw->addIdentifierQuotes( self::getBucketTableName( $bucketName ) );
		$res = $dbw->query( "SHOW FULL COLUMNS FROM $dbTableName;", __METHOD__ );

		$fields = [];
		foreach ( $res as $val ) {
			$fields[] = BucketSchemaField::fromJson( $val->Field, $val->Comment );
		}
		return new BucketSchema( $bucketName, $fields );
	}

	public static function createOrModifyTable( string $bucketName, object $jsonSchema, bool $isExistingPage ): void {
		$bucketName = Bucket::getValidBucketName( $bucketName );
		$newSchema = [
			'_page_id' => new BucketSchemaField( '_page_id', BucketValueType::Integer, false, false ),
			'_index' => new BucketSchemaField( '_index', BucketValueType::Integer, false, false ),
			'page_name' => new BucketSchemaField( 'page_name', BucketValueType::Page, true, false ),
			'page_name_sub' => new BucketSchemaField( 'page_name_sub', BucketValueType::Page, true, false )
		];

		if ( $bucketName === Bucket::ISSUES_BUCKET ) {
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

			$valueType = BucketValueType::tryFrom( $fieldData->type );
			if ( $valueType === null ) {
				throw new SchemaException(
					wfMessage( 'bucket-schema-invalid-data-type', $fieldName, $fieldData->type ) );
			}

			$index = true;
			if ( isset( $fieldData->index ) ) {
				$index = boolval( $fieldData->index );
			}

			$repeated = false;
			if ( isset( $fieldData->repeated ) ) {
				$repeated = boolval( $fieldData->repeated );
			}

			if ( $repeated === true && $index === false ) {
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
			if ( !$dbw->tableExists( $bucketSchema->getTableName() ) ) {
				// We are a new bucket json
				$statement = self::getCreateTableStatement( $bucketSchema, $dbw );
				$bucketDBuser = self::getBucketDBUser();
				$escapedTableName = $bucketSchema->getSafe( $dbw );
				// Note: The main database connection is only used to grant access to the new table.
				MediaWikiServices::getInstance()->getDBLoadBalancer()
					->getConnection( DB_PRIMARY )->query( "GRANT ALL ON $escapedTableName TO $bucketDBuser;" );
			} else {
				// We are an existing bucket json
				$oldSchema = self::buildSchemaFromComments( $bucketSchema->getName(), $dbw );
				$statement = self::getAlterTableStatement( $bucketSchema, $oldSchema, $dbw );
			}
			$dbw->query( $statement );

			// At this point is is possible that another transaction has changed the table so we start a transaction,
			// read the column comments (which are the schema), and write that to bucket_schemas
			$dbw->begin();
			$commentSchema = self::buildSchemaFromComments( $bucketSchema->getName(), $dbw );
			// If the schema contained in the comments is eqivalent to what we expect (ignoring field ordering)
			// then we just write the expected schema, to preserve the ordering from the MW page.
			$expectedFields = $bucketSchema->getFields();
			$actualFields = $commentSchema->getFields();
			$expectedFieldsSorted = sort( $expectedFields );
			$actualFieldsSorted = sort( $actualFields );
			if ( json_encode( $expectedFieldsSorted ) === json_encode( $actualFieldsSorted ) ) {
				$schemaJson = json_encode( $bucketSchema );
			} else {
				// If for some reason the schemas are not eqivalent, write the schema that matches the DB state
				$schemaJson = json_encode( $commentSchema );
			}
			$dbw->upsert(
				'bucket_schemas',
				[ 'bucket_name' => $bucketSchema->getName(), 'schema_json' => $schemaJson ],
				'bucket_name',
				[ 'schema_json' => $schemaJson ]
			);
			$dbw->commit();
		}, __METHOD__ );
	}

	private static function getAlterTableStatement(
		BucketSchema $bucketSchema, BucketSchema $oldSchema, IDatabase $dbw
	): string {
		$alterTableFragments = [];

		$oldFields = $oldSchema->getFields();
		foreach ( $bucketSchema->getFields() as $fieldName => $field ) {
			$escapedFieldName = $dbw->addIdentifierQuotes( $fieldName );
			$fieldJson = $dbw->addQuotes( json_encode( $field ) );
			$newDbType = $field->getDatabaseValueType()->value;
			$newColumn = true;
			if ( isset( $oldFields[$fieldName] ) ) {
				$oldDbType = $oldFields[$fieldName]->getDatabaseValueType()->value;
				$newColumn = false;
			}
			$typeChange = ( $oldDbType !== $newDbType );

			if ( $newColumn === false ) {
				# If the old schema has an index, check if it needs to be dropped
				if ( $oldFields[$fieldName]->getIndexed() ) {
					if ( $typeChange || $field->getIndexed() === false ) {
						$alterTableFragments[] = "DROP INDEX $escapedFieldName";
					}
				}
				# Always drop and then re-add the column for field type changes.
				if ( $typeChange ) {
					$alterTableFragments[] = "DROP $escapedFieldName";
				}
			}

			if ( $newColumn || $typeChange ) {
				$alterTableFragments[] = "ADD $escapedFieldName " . $newDbType . " COMMENT $fieldJson";
			} else {
				// If an existing column has the same DB type, check for a change between TEXT/PAGE,
				// or a change to the index.
				if (
					( $oldFields[$fieldName]->getType() !== $field->getType() )
					|| ( $field->getIndexed() !== $oldFields[$fieldName]->getIndexed() )
				) {
					# Acts as a no-op except to update the comment
					$alterTableFragments[] = "MODIFY $escapedFieldName " . $newDbType . " COMMENT $fieldJson";
				}
			}
			if ( $field->getIndexed() ) {
				if ( $newColumn || $typeChange || $oldFields[$fieldName]->getIndexed() === false ) {
					$alterTableFragments[] = 'ADD ' . self::getIndexStatement( $field, $dbw );
				}
			}
			unset( $oldFields[$fieldName] );
		}
		// Drop unused columns
		foreach ( $oldFields as $deletedColumn => $val ) {
			$escapedDeletedColumn = $dbw->addIdentifierQuotes( $deletedColumn );
			if ( $val->getRepeated() === true ) {
				// We must explicitly drop indexes for repeated fields
				$alterTableFragments[] = "DROP INDEX $escapedDeletedColumn";
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
			$createTableFragments[] =
				"{$dbw->addIdentifierQuotes($field->getFieldName())} $dbType COMMENT $fieldJson";
			if ( $field->getIndexed() ) {
				$createTableFragments[] = self::getIndexStatement( $field, $dbw );
			}
		}
		$createTableFragments[] =
			"PRIMARY KEY ({$dbw->addIdentifierQuotes('_page_id')}, {$dbw->addIdentifierQuotes('_index')})";

		$dbTableName = $dbw->addIdentifierQuotes( $newSchema->getTableName() );
		return "CREATE TABLE $dbTableName (" . implode( ', ', $createTableFragments ) . ') DEFAULT CHARSET=utf8mb4;';
	}

	public static function countPagesUsingBucket( string $bucketName ): int {
		$dbw = self::getDB();
		$bucketName = Bucket::getValidBucketName( $bucketName );
		return $dbw->newSelectQueryBuilder()
			->table( 'bucket_pages' )
			->lockInShareMode()
			->where( [ 'bucket_name' => $bucketName ] )
			->fetchRowCount();
	}

	public static function deleteTable( string $bucketName ): void {
		$dbw = self::getDB();
		$bucketName = Bucket::getValidBucketName( $bucketName );
		$tableName = $dbw->addIdentifierQuotes( self::getBucketTableName( $bucketName ) );

		$dbw->newDeleteQueryBuilder()
			->table( 'bucket_schemas' )
			->where( [ 'bucket_name' => $bucketName ] )
			->caller( __METHOD__ )
			->execute();
		$dbw->query( "DROP TABLE IF EXISTS $tableName" );
	}

	private static function getIndexStatement( BucketSchemaField $field, IDatabase $dbw ): string {
		$fieldName = $dbw->addIdentifierQuotes( $field->getFieldName() );
		switch ( $field->getDatabaseValueType() ) {
			case DatabaseValueType::Json:
				// Typecasting for repeated fields doesn't give us any advantage
				return "INDEX $fieldName((CAST($fieldName AS CHAR(512) ARRAY)))";
			case DatabaseValueType::Text:
				return "INDEX $fieldName($fieldName(255))";
			default:
				return "INDEX $fieldName($fieldName)";
		}
	}

	/**
	 * @param string $bucketName
	 * @return string
	 */
	public static function getBucketTableName( $bucketName ): string {
		return 'bucket__' . $bucketName;
	}

}
