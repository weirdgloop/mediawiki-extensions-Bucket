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
			'tablePrefix' => $mainDB->tablePrefix(),
			// Weird Gloop specific tweak
			'defaultMaxExecutionTimeForQueries' => 500
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
		$dbTableName = $dbw->tableName( self::getBucketTableName( $bucketName ) );
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

			$lcFieldName = Bucket::getValidFieldName( $bucketName, $fieldName );

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
				$statements = self::getCreateTableStatement( $bucketSchema, $dbw );
			} else {
				// We are an existing bucket json
				$oldSchema = self::buildSchemaFromComments( $bucketSchema->getName(), $dbw );
				$statements = self::getAlterTableStatement( $bucketSchema, $oldSchema, $dbw );
			}
			foreach ( $statements as $table ) {
				// TODO I don't like this if, but we probably don't want to do
				// grants on every modified table every time?
				if ( $table[0] !== '' ) {
					$bucketDBuser = self::getBucketDBUser();
					$escapedTableName = $dbw->tableName( $table[0] );
					// Note: The main database connection is only used to grant access to the new table.
					MediaWikiServices::getInstance()->getDBLoadBalancer()
						->getConnection( DB_PRIMARY )->query( "GRANT ALL ON $escapedTableName TO $bucketDBuser;" );
				}
				$dbw->query( $table[1] );
			}

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
	): array {
		$alterTableFragments = [];
		$tableStatements = [];

		$oldFields = $oldSchema->getFields();
		foreach ( $bucketSchema->getFields() as $fieldName => $field ) {
			$escapedFieldName = $dbw->addIdentifierQuotes( $fieldName );
			$fieldJson = $dbw->addQuotes( json_encode( $field ) );
			$newDbType = $field->getDatabaseValueType()->value;
			$newColumn = true;
			$oldField = null;
			if ( isset( $oldFields[$fieldName] ) ) {
				$oldField = $oldFields[$fieldName];
				$newColumn = false;
			}
			// Check if the type has changed either for the regular or repeated column
			$typeChange =
				$oldField !== null &&
				( ( $field->getSubDatabaseValueType() !== $oldField->getSubDatabaseValueType() )
				|| ( $field->getDatabaseValueType() !== $oldField->getDatabaseValueType() ) );

			if ( $newColumn === false ) {
				# If the old schema has an index, check if it needs to be dropped
				if ( $oldField !== null && $oldField->getIndexed() && !$oldField->getRepeated() ) {
					if ( $typeChange || $field->getIndexed() === false ) {
						$alterTableFragments[] = "DROP INDEX $escapedFieldName";
					}
				}
				# Always drop and then re-add the column for field type changes.
				if ( $typeChange ) {
					$alterTableFragments[] = "DROP $escapedFieldName";
					if ( $oldField !== null && $oldField->getRepeated() ) {
						$repeatedTableName = self::getRepeatedFieldTableName( $bucketSchema->getName(), $fieldName );
						$tableStatements[] = [
							'',
							"DROP TABLE IF EXISTS $repeatedTableName;"
						];
					}
				}
			}

			if ( $newColumn || $typeChange ) {
				$alterTableFragments[] = "ADD $escapedFieldName $newDbType COMMENT $fieldJson";
				if ( $field->getRepeated() ) {
					$tableStatements[] = self::getCreateRepeatedTableStatement( $bucketSchema, $field, $dbw );
				}
			} else {
				// If an existing column has the same DB type, check for a change between TEXT/PAGE,
				// or a change to the index.
				if (
					( $oldFields[$fieldName]->getType() !== $field->getType() )
					|| ( $field->getIndexed() !== $oldFields[$fieldName]->getIndexed() )
				) {
					# Acts as a no-op except to update the comment
					$alterTableFragments[] = "MODIFY $escapedFieldName $newDbType COMMENT $fieldJson";
				}
			}
			if ( $field->getIndexed() && !$field->getRepeated() ) {
				if ( $newColumn || $typeChange || $oldFields[$fieldName]->getIndexed() === false ) {
					$alterTableFragments[] = 'ADD ' . self::getIndexStatement( $field, $dbw );
				}
			}
			unset( $oldFields[$fieldName] );
		}
		// Drop unused columns
		foreach ( $oldFields as $deletedColumn => $val ) {
			$escapedDeletedColumn = $dbw->addIdentifierQuotes( $deletedColumn );
			$alterTableFragments[] = "DROP $escapedDeletedColumn";
			if ( $val->getRepeated() === true ) {
				$repeatedTableName = self::getRepeatedFieldTableName( $bucketSchema->getName(), $val->getFieldName() );
				$tableStatements[] = [
					'',
					"DROP TABLE IF EXISTS $repeatedTableName;"
				];
			}
		}

		$dbTableName = $dbw->tableName( $bucketSchema->getTableName() );
		$tableStatements[] = [
			'', // The table name used for granting permissions, but an altered table already has permissions
			"ALTER TABLE $dbTableName " . implode( ', ', $alterTableFragments ) . ';'
		];

		return $tableStatements;
	}

	private static function getCreateTableStatement( BucketSchema $newSchema, IDatabase $dbw ): array {
		$createTableFragments = [];
		$tableStatements = [];

		foreach ( $newSchema->getFields() as $field ) {
			$dbType = $field->getDatabaseValueType()->value;
			$fieldJson = $dbw->addQuotes( json_encode( $field ) );
			if ( $field->getRepeated() === true ) {
				$tableStatements[] = self::getCreateRepeatedTableStatement( $newSchema, $field, $dbw );
			}
			$createTableFragments[] =
				"{$dbw->addIdentifierQuotes($field->getFieldName())} $dbType COMMENT $fieldJson";
			if ( $field->getIndexed() && !$field->getRepeated() ) {
				$createTableFragments[] = self::getIndexStatement( $field, $dbw );
			}
		}
		$createTableFragments[] =
			"PRIMARY KEY ({$dbw->addIdentifierQuotes('_page_id')}, {$dbw->addIdentifierQuotes('_index')})";
		$dbTableName = $dbw->tableName( $newSchema->getTableName() );

		$tableStatements[] = [
			$dbTableName,
			"CREATE TABLE $dbTableName (" . implode( ', ', $createTableFragments ) . ') DEFAULT CHARSET=utf8mb4;'
		];

		return $tableStatements;
	}

	/**
	 * Temporarily public for migration
	 */
	public static function getCreateRepeatedTableStatement(
		BucketSchema $newSchema, BucketSchemaField $originalField, IDatabase $dbw ): array {
		$createTableFragments = [];
		$repeatedSchema = [
			new BucketSchemaField( '_page_id', BucketValueType::Integer, true, false ),
			new BucketSchemaField( '_index', BucketValueType::Integer, true, false ),
			$originalField
		];

		foreach ( $repeatedSchema as $field ) {
			$dbType = $field->getSubDatabaseValueType()->value;
			$fragment = "{$dbw->addIdentifierQuotes($field->getFieldName())} $dbType";
			$createTableFragments[] = $fragment;
			if ( $field->getIndexed() ) {
				$createTableFragments[] = self::getIndexStatement( $field, $dbw );
			}
		}

		// Create a key to match the main table primary key
		$createTableFragments[] = 'INDEX idx_page_index (_page_id, _index)';
		$dbTableName = self::getRepeatedFieldTableName( $newSchema->getName(), $originalField->getFieldName() );
		return [
			$dbTableName,
			"CREATE TABLE $dbTableName (" . implode( ', ', $createTableFragments ) . ') DEFAULT CHARSET=utf8mb4;'
		];
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
		$res = $dbw->newSelectQueryBuilder()
			->from( 'bucket_schemas' )
			->select( [ 'bucket_name', 'schema_json' ] )
			->lockInShareMode()
			->where( [ 'bucket_name' => $bucketName ] )
			->caller( __METHOD__ )
			->fetchRow();
		if ( $res !== false ) {
			$schema = new BucketSchema(
				$res->bucket_name,
				json_decode( $res->schema_json, true )
			);
			$deleteTableNames = self::getRelatedTableNames( $bucketName, $schema );
			foreach ( $deleteTableNames as $name ) {
				$dbw->dropTable( $name );
			}
		}
		$dbw->newDeleteQueryBuilder()
			->table( 'bucket_schemas' )
			->where( [ 'bucket_name' => $bucketName ] )
			->caller( __METHOD__ )
			->execute();
	}

	private static function getIndexStatement( BucketSchemaField $field, IDatabase $dbw ): string {
		$fieldName = $dbw->addIdentifierQuotes( $field->getFieldName() );
		switch ( $field->getSubDatabaseValueType() ) {
			case DatabaseValueType::Text:
				// More than 40 characters can cause a MySQL error 1713: Undo log record is too big.
				return "INDEX $fieldName($fieldName(40))";
			default:
				return "INDEX $fieldName($fieldName)";
		}
	}

	public static function getBucketTableName( string $bucketName ): string {
		return 'bucket__' . $bucketName;
	}

	public static function getRepeatedFieldTableName( string $bucketName, string $fieldName ): string {
		return 'bucket__' . $bucketName . '__' . $fieldName;
	}

	public static function getRelatedTableNames( string $bucketName, BucketSchema $schema ): array {
		$names = [ self::getBucketTableName( $bucketName ) ];
		foreach ( $schema->getFields() as $field ) {
			if ( $field->getRepeated() ) {
				$names[] = self::getRepeatedFieldTableName( $bucketName, $field->getFieldName() );
			}
		}
		return $names;
	}
}
