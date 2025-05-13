<?php

namespace MediaWiki\Extension\Bucket;

use MediaWiki\MediaWikiServices;
use LogicException;
use Wikimedia\Rdbms\IDatabase;

class Bucket {
	public const EXTENSION_DATA_KEY = 'bucket:puts';
	public const EXTENSION_PROPERTY_KEY = 'bucketputs';
	public const MAX_LIMIT = 5000;
	public const DEFAULT_LIMIT = 500;
	public const MESSAGE_BUCKET = 'bucket_message';

	private static $dataTypes = [
		'BOOLEAN' => 'BOOLEAN',
		'DOUBLE' => 'DOUBLE',
		'INTEGER' => 'INTEGER',
		'JSON' => 'JSON',
		'TEXT' => 'TEXT',
		'PAGE' => 'TEXT'
	];

	private static $requiredColumns = [
			'_page_id' => [ 'type' => 'INTEGER', 'index' => false , 'repeated' => false ],
			'_index' => [ 'type' => 'INTEGER', 'index' => false , 'repeated' => false ],
			'page_name' => [ 'type' => 'PAGE', 'index' => true,  'repeated' => false ],
			'page_name_sub' => [ 'type' => 'PAGE', 'index' => true, 'repeated' => false],
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

	public static function logMessage( string $bucket, string $property, string $type, string $message, &$logs) {
		//TODO need to create the correct bucket on plugin install
		if ( !array_key_exists( self::MESSAGE_BUCKET, $logs ) ) {
			$logs[self::MESSAGE_BUCKET] = [];
		}
		if ( $bucket != "" ) {
			$bucket = "Bucket:" . $bucket;
		}
		$logs[self::MESSAGE_BUCKET][] = [
			"sub" => "",
			"data" => [
				"bucket" => $bucket,
				"property" => $property,
				"type" => wfMessage($type),
				"message" => $message
			]
		];
	}

	/*
	Called when a page is saved containing a bucket.put
	*/
	public static function writePuts( int $pageId, string $titleText, array $puts, bool $writingLogs = false) {
		// file_put_contents( MW_INSTALL_PATH . '/cook.txt', "writePuts start " . print_r($puts, true) . "\n" , FILE_APPEND);
		$logs = [];
		$dbw = MediaWikiServices::getInstance()->getDBLoadBalancer()->getConnectionRef( DB_PRIMARY );

		$res =  $dbw->newSelectQueryBuilder()
				->from( 'bucket_pages' )
				->select( [ '_page_id', 'table_name', 'put_hash' ] )
				->where( [ '_page_id' => $pageId ] )
				->caller( __METHOD__ )
				->fetchResultSet();
		$bucket_hash = [];
		foreach ( $res as $row ) {
			$bucket_hash[ $row->table_name ] = $row->put_hash;
		}

		//Combine existing written bucket list and new written bucket list.
		$relevantBuckets = array_merge(array_keys($puts), array_keys($bucket_hash));
		$res = $dbw->newSelectQueryBuilder()
				->from( 'bucket_schemas' )
				->select( [ 'table_name', 'backing_table_name', 'schema_json' ] )
				->where( ['table_name' => $relevantBuckets ] )
				->caller( __METHOD__ )
				->fetchResultSet();
		$schemas = [];
		$backingBucketName = []; //Used to generate warning messages when writing to a view
		$backingBuckets = []; //Used to generate error when a written view and bucket are pointing to the same location
		foreach ( $res as $row ) {
			$schemas[$row->table_name] = json_decode($row->schema_json, true);
			$backingBucketName[$row->table_name] = $row->backing_table_name;
			$realTable = $row->backing_table_name ?? $row->table_name;
			if (isset($puts[$row->table_name])) {
				if (array_key_exists($realTable, $backingBuckets)) {
					self::logMessage($row->table_name, "", "bucket-general-error", wfMessage("bucket-double-write-redirect-error", $row->table_name, $backingBuckets[$realTable]), $logs);
					unset($puts[$row->table_name]);
				} else {
					$backingBuckets[$realTable] = $row->table_name;
				}
			}
		}

		foreach ( $puts as $tableName => $tableData ) {
			if ($tableName == "") {
				self::logMessage($tableName, "", "bucket-general-error", wfMessage("bucket-no-bucket-defined-warning"), $logs);
				continue;
			}

			$tableNameTmp = Bucket::getValidFieldName($tableName);
			if ( $tableNameTmp == false ) {
				self::logMessage($tableName, "", "bucket-general-warning", wfMessage("bucket-invalid-name-warning", $tableName), $logs);
				continue;
			}
			if ( $tableNameTmp != $tableName) {
				self::logMessage($tableName, "", "bucket-general-warning", wfMessage("bucket-capital-name-warning"), $logs);
			}
			$tableName = $tableNameTmp;

			if (array_key_exists($tableName, $backingBucketName) && $backingBucketName[$tableName] != null) {
				self::logMessage($tableName, "", "bucket-general-warning", wfMessage("bucket-redirect-write-update-warning", $tableName, $backingBucketName[$tableName]), $logs);
			}

			if (!array_key_exists($tableName, $schemas)) {
				self::logMessage($tableName, "", "bucket-general-error", wfMessage("bucket-no-exist-error"), $logs);
				continue;
			}

			$tablePuts = [];
			$dbTableName = 'bucket__' . $tableName;
			$res = $dbw->newSelectQueryBuilder()
				->from( $dbw->addIdentifierQuotes( $dbTableName ) )
				->select( "*" )
				->where( [ '_page_id' => $pageId ] )
				->caller( __METHOD__ )
				->fetchResultSet();

			$fields = [];
			$fieldNames = $res->getFieldNames();
			foreach ( $fieldNames as $fieldName ) {
				// TODO: match on type, not just existence
				$fields[ $fieldName ] = true;
			}
			foreach ( $tableData as $idx => $singleData) {
				$sub = $singleData['sub'];
				$singleData = $singleData['data'];
				if (gettype($singleData) != "array") {
					self::logMessage($tableName, "", "bucket-general-error", wfMessage("bucket-put-syntax-error"), $logs);
					continue;
				}
				foreach ( $singleData as $key => $value ) {
					if ( !isset($fields[$key]) || !$fields[$key] ) {
						self::logMessage($tableName, $key, "bucket-general-warning", wfMessage("bucket-put-key-missing-warning", $key, $tableName), $logs);
					}
				}
				$singlePut = [];
				foreach ( $fields as $key => $_ ) {
					$value = isset($singleData[$key]) ? $singleData[$key] : null;
					// file_put_contents(MW_INSTALL_PATH . '/cook.txt', print_r($value, true) . " ==========$key===========\n", FILE_APPEND);
					#TODO JSON relies on forcing utf8 transmission in DatabaseMySQL.php line 829
					$singlePut[$dbw->addIdentifierQuotes($key)] = self::castToDbType($value, self::getDbType($fieldName, $schemas[$tableName][$key]));
				}
				$singlePut[$dbw->addIdentifierQuotes('_page_id')] = $pageId;
				$singlePut[$dbw->addIdentifierQuotes('_index')] = $idx;
				$singlePut[$dbw->addIdentifierQuotes('page_name')] = $titleText;
				$singlePut[$dbw->addIdentifierQuotes('page_name_sub')] = $titleText;
				if ( isset( $sub ) && strlen($sub) > 0) {
					$singlePut[$dbw->addIdentifierQuotes('page_name_sub')] = $titleText . '#' . $sub;
				}
				$tablePuts[$idx] = $singlePut;
			}

			#Check these puts against the hash of the last time we did puts.
			sort($tablePuts);
			sort($schemas[$tableName]);
			$newHash = hash( 'sha256', json_encode( $tablePuts ) . json_encode($schemas[$tableName]));
			if ( isset($bucket_hash[ $tableName ]) && $bucket_hash[ $tableName ] == $newHash ) {
				file_put_contents(MW_INSTALL_PATH . '/cook.txt', "HASH MATCH SKIPPING WRITING $tableName $titleText =====================\n", FILE_APPEND);
				unset( $bucket_hash[ $tableName ] );
				continue;
			}
			file_put_contents(MW_INSTALL_PATH . '/cook.txt', "WRITING $tableName $titleText \n", FILE_APPEND);

			//Remove the bucket_hash entry so we can it as a list of removed buckets at the end.
			unset( $bucket_hash[ $tableName ] );

			// file_put_contents( MW_INSTALL_PATH . '/cook.txt', "writePuts puts " . print_r(json_encode($tablePuts, JSON_PRETTY_PRINT), true) . "\n" , FILE_APPEND);
			// TODO: does behavior here depend on DBO_TRX?
			$dbw->begin();
			$dbw->newDeleteQueryBuilder()
				->deleteFrom( $dbw->addIdentifierQuotes($dbTableName) )
				->where( [ '_page_id' => $pageId ] )
				->caller( __METHOD__ )
				->execute();
			$dbw->newInsertQueryBuilder()
				->insert( $dbw->addIdentifierQuotes($dbTableName) )
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

			$dbw->commit();
			// file_put_contents( MW_INSTALL_PATH . '/cook.txt', "commited\n", FILE_APPEND );
		}


		if ( !$writingLogs ) {
			//Clean up bucket_pages entries for buckets that are no longer written to on this page.
			$tablesToDelete = array_keys( array_filter( $bucket_hash ) );
			if ( count($logs) != 0 ) {
				unset($tablesToDelete[Bucket::MESSAGE_BUCKET]);
			} else {
				$tablesToDelete[] = Bucket::MESSAGE_BUCKET;
			}

			if ( count($tablesToDelete) > 0 ) {
				$dbw->begin(__METHOD__);
				$dbw->newDeleteQueryBuilder()
					->deleteFrom('bucket_pages')
					->where(['_page_id' => $pageId, 'table_name' => $tablesToDelete])
					->caller(__METHOD__)
					->execute();
				foreach ($tablesToDelete as $name) {
					$isView = isset($backingBucketName[$name]);
					//If we aren't a view and we aren't a backing bucket, or we are a view and our backing bucket isn't also written to
					$shouldDelete = (!$isView && !in_array($name, $backingBucketName)) || ($isView && !array_key_exists($backingBucketName[$name], $schemas));
					if ( $shouldDelete ) {
						$dbw->newDeleteQueryBuilder()
							->deleteFrom($dbw->addIdentifierQuotes('bucket__' . $name))
							->where(['_page_id' => $pageId])
							->caller(__METHOD__)
							->execute();
					}
					if ( $isView ) {
						$viewUses = $dbw->newSelectQueryBuilder()
							->from('bucket_pages')
							->where(['table_name' => $name])
							->caller(__METHOD__)
							->fetchRowCount();
						if ($viewUses == 0) {
							$dbw->newDeleteQueryBuilder()
								->table("bucket_schemas")
								->where(["table_name" => $name])
								->caller(__METHOD__)
								->execute();
							$dbw->query("DROP VIEW IF EXISTS " . $dbw->addIdentifierQuotes('bucket__' . $name));
							file_put_contents(MW_INSTALL_PATH . '/cook.txt', "DROPPING VIEW $name with $viewUses \n", FILE_APPEND);
						}
					}
				}
				$dbw->commit(__METHOD__);
			}
			
			if ( count($logs) > 0 ) {
				self::writePuts($pageId, $titleText, $logs, true);
			}
		}
	}

	/**
	 * Called for any page save that doesn't have bucket puts
	 */
	public static function clearOrphanedData( int $pageId) {
		$dbw = MediaWikiServices::getInstance()->getDBLoadBalancer()->getConnectionRef( DB_PRIMARY );

		//Check if any buckets are storing data for this page
		$res =  $dbw->newSelectQueryBuilder()
				->from( 'bucket_pages' )
				->select( [ 'table_name' ] )
				->where( [ '_page_id' => $pageId ] )
				->groupBy( 'table_name' )
				->caller( __METHOD__ )
				->fetchResultSet();

		//If there is data associated with this page, delete it.
		if( $res->count() > 0 ) {
			$dbw->newDeleteQueryBuilder()
				->deleteFrom('bucket_pages')
				->where(['_page_id' => $pageId])
				->caller(__METHOD__)
				->execute();
			$table = [];
			foreach ( $res as $row ) {
				$table[] = $row->table_name;
			}
			$res = $dbw->newSelectQueryBuilder()
				->from( 'bucket_schemas' )
				->select( [ 'table_name', 'backing_table_name' ] )
				->where( ['table_name' => array_unique($table) ] )
				->caller( __METHOD__ )
				->fetchResultSet();
			$isView = [];
			foreach ($res as $row) {
				$isView[$row->table_name] = $row->backing_table_name;
			}

			foreach ( $table as $name ) {
				//Clear this pages data from the bucket
				$dbw->newDeleteQueryBuilder()
					->deleteFrom($dbw->addIdentifierQuotes('bucket__' . $name ))
					->where(['_page_id' => $pageId])
					->caller(__METHOD__)
					->execute();
				
				//If the bucket is a view and now empty, delete the view
				if (isset($isView[$name])) {
					$viewUses = $dbw->newSelectQueryBuilder()
						->from('bucket_pages')
						->where(['table_name' => $name])
						->caller(__METHOD__)
						->fetchRowCount();
					if ($viewUses == 0) {
						$dbw->newDeleteQueryBuilder()
							->table("bucket_schemas")
							->where(["table_name" => $name])
							->caller(__METHOD__)
							->execute();
						$dbw->query("DROP VIEW IF EXISTS " . $dbw->addIdentifierQuotes('bucket__' . $name));
						file_put_contents(MW_INSTALL_PATH . '/cook.txt', "DROPPING VIEW $name with $viewUses \n", FILE_APPEND);
					}
				}
			}
		}
	}

	public static function getValidFieldName( string $fieldName ) {
		if ( preg_match( '/^[a-zA-Z0-9_ ]+$/', $fieldName ) ) {
			return str_replace(" ", "_", strtolower( trim( $fieldName ) ));
		}
		return false;
	}

	private static function getValidBucketName( string $bucketName ) {
		if ( ucfirst($bucketName) != ucfirst(strtolower($bucketName))) {
			throw new SchemaException( wfMessage("bucket-capital-name-error") );
		}
		$bucketName = self::getValidFieldName( $bucketName );
		if ( !$bucketName ) {
			throw new SchemaException( wfMessage("bucket-invalid-name-warning", $bucketName) );
		}
		return $bucketName;
	}

	public static function canCreateTable( string $bucketName ) {
		$bucketName = self::getValidBucketName($bucketName);
		$dbw = MediaWikiServices::getInstance()->getDBLoadBalancer()->getConnectionRef( DB_PRIMARY );
		if (!$dbw->selectField( 'bucket_schemas', [ 'schema_json' ], [ 'table_name' => $bucketName ] )) {
			return true;
		} else {
			return false;
		}
	}

	public static function createOrModifyTable( string $bucketName, object $jsonSchema, int $parentId ) {
		$newSchema = array_merge( [], self::$requiredColumns );

		if ( empty( (array)$jsonSchema ) ) {
			throw new SchemaException( wfMessage("bucket-schema-no-columns-error") );
		}

		$bucketName = self::getValidBucketName($bucketName);

		foreach ( $jsonSchema as $fieldName => $fieldData ) {
			if ( gettype( $fieldName ) !== 'string' ) {
				throw new SchemaException( wfMessage("bucket-schema-must-be-strings", $fieldName) );
			}

			$lcFieldName = self::getValidFieldName( $fieldName );
			if ( !$lcFieldName ) {
				throw new SchemaException( wfMessage("bucket-schema-invalid-field-name", $fieldName) );
			}

			$lcFieldName = strtolower( $fieldName );
			if ( isset( $newSchema[$lcFieldName] ) ) {
				throw new SchemaException( wfMessage("bucket-schema-duplicated-field-name", $fieldName) );
			}

			if ( !isset( self::$dataTypes[$fieldData->type] ) ) {
				throw new SchemaException( wfMessage("bucket-schema-invalid-data-type", $fieldName, $fieldData->type) );
			}

			$index = true;
			if ( isset( $fieldData->index ) ) {
				$index = boolval( $fieldData->index );
			}

			$repeated = false;
			if ( isset( $fieldData->repeated ) ) {
				$repeated = boolval( $fieldData-> repeated );
			}

			$newSchema[$lcFieldName] = [ 'type' => $fieldData->type, 'index' => $index, 'repeated' => $repeated ];
		}
		$dbTableName = 'bucket__' . $bucketName;
		$dbw = MediaWikiServices::getInstance()->getDBLoadBalancer()->getConnectionRef( DB_PRIMARY );

		$oldSchema = $dbw->selectField( 'bucket_schemas', [ 'schema_json' ], [ 'table_name' => $bucketName ] );
		if ($oldSchema && $parentId == 0) {
			//An existing bucket json with a parent id of 0 means we are trying to create a new bucket at a location with an active view.
			if ( Bucket::isBucketWithPuts($bucketName, $dbw) ) {
				throw new SchemaException( wfMessage("bucket-schema-create-over-redirect-error") );
			}
			file_put_contents(MW_INSTALL_PATH . '/cook.txt', "OVERWRITING SCHEMA FOR UNUSED VIEW \n", FILE_APPEND);
			$dbw->query("DROP VIEW IF EXISTS `bucket__$bucketName`");
			$oldSchema = false;
		}
		if ( !$oldSchema ) {
			//We are a new bucket json
			$statement = self::getCreateTableStatement( $dbTableName, $newSchema );
			file_put_contents(MW_INSTALL_PATH . '/cook.txt', "CREATE TABLE STATEMENT $statement \n", FILE_APPEND);
			$dbw->query( $statement );
		} else {
			//We are an existing bucket json
			$oldSchema = json_decode( $oldSchema, true );
			$statement = self::getAlterTableStatement( $dbTableName, $newSchema, $oldSchema, $dbw );
			file_put_contents(MW_INSTALL_PATH . '/cook.txt', "ALTER TABLE STATEMENT $statement \n", FILE_APPEND);
			$dbw->query( $statement );
		}
		$newSchema["_parent_rev_id"] = $parentId;
		$schemaJson = json_encode( $newSchema );
		$dbw->upsert(
			'bucket_schemas',
			[ 'table_name' => $bucketName, 'schema_json' => $schemaJson ],
			'table_name',
			[ 'schema_json' => $schemaJson ]
		);
	}

	public static function deleteTable( $bucketName ) {
		$dbw = MediaWikiServices::getInstance()->getDBLoadBalancer()->getConnectionRef( DB_PRIMARY );
		$bucketName = self::getValidBucketName($bucketName);
		$tableName = "bucket__" . $bucketName;

		if (Bucket::canDeleteBucketPage($bucketName)) {
			$dbw->newDeleteQueryBuilder()
				->table("bucket_schemas")
				->where(["table_name" => $bucketName])
				->caller(__METHOD__)
				->execute();
			$dbw->query("DROP TABLE IF EXISTS $tableName");
		} else {
			//TODO: Throw error?
		}
	}
	
	public static function canDeleteBucketPage( $bucketName ) {
		$dbw = MediaWikiServices::getInstance()->getDBLoadBalancer()->getConnectionRef( DB_PRIMARY );
		$bucketName = self::getValidBucketName($bucketName);
		$tableName = "bucket__" . $bucketName;
		//Check that table actually exists
		$res = $dbw->query("SHOW FULL TABLES LIKE {$dbw->addQuotes($tableName)}");
		if ( $res->count() == 0 ) {
			return true;
		}
		//Get all bucket names that point to this table
		//TODO: Probably doesn't matter much, but surely this can be a subquery or something
		$bucketNames = $dbw->newSelectQueryBuilder()
							->table("bucket_schemas")
							->select("table_name")
							->where($dbw->makeList(["table_name" => $bucketName, "backing_table_name" => $bucketName], LIST_OR))
							->caller(__METHOD__)
							->fetchFieldValues();
		$putCount = $dbw->newSelectQueryBuilder()
						->table("bucket_pages")
						->where(["table_name" => $bucketNames])
						->fetchRowCount();
		if ( $putCount > 0 ) { 
			return false;
		}
		return true;
	}

	public static function isBucketWithPuts( $cleanBucketName, IDatabase $dbw ) {
		return $dbw->newSelectQueryBuilder()->table("bucket_pages")->where(["table_name" => $cleanBucketName])->fetchRowCount() !== 0;
	}

	/**
	 * @return string|bool - String if the move will fail. True if the move is good and on top of a view, false if the move is good but there is no view.
	 */
	public static function canMoveBucket( $bucketName, $newBucketName ) {
		$dbw = MediaWikiServices::getInstance()->getDBLoadBalancer()->getConnectionRef( DB_PRIMARY );

		$bucketName = self::getValidBucketName($bucketName);
		$newBucketName = self::getValidBucketName($newBucketName);

		$res = $dbw->newSelectQueryBuilder()
			->table("bucket_schemas")
			->select(["table_name", "backing_table_name", "schema_json"])
			->where($dbw->makeList(["table_name" => [$bucketName, $newBucketName], "backing_table_name" => [$bucketName, $newBucketName]], LIST_OR))
			->fetchResultSet();

		$existingBuckets = [];
		foreach ($res as $row) {
			$existingBuckets[$row->table_name] = $row;
			file_put_contents(MW_INSTALL_PATH . '/cook.txt', "CAN MOVE BUCKET? " . print_r($row, true) . " \n", FILE_APPEND);
		}

		//The only way we have more than 1 is if theres a view pointing to this bucket we are trying to move.
		if ( count($existingBuckets) > 1) {
			file_put_contents(MW_INSTALL_PATH . '/cook.txt', " ASDFASDF " . print_r($existingBuckets, true) . " \n", FILE_APPEND);
			if (!isset($existingBuckets[$bucketName]) || !isset($existingBuckets[$newBucketName])) {
				return wfMessage("bucket-move-existing-redirect-error");
			}
		}

		//Check that old table actually exists
		if ( !array_key_exists($bucketName, $existingBuckets) ) {
			return wfMessage("bucket-no-exist", $bucketName);
		}
		//Check that new table doesn't already exist
		//OR if it exists and is a view and no buckets write to it then we are good
		$needToDropView = false;
		if ( array_key_exists($newBucketName, $existingBuckets) ) {
			file_put_contents(MW_INSTALL_PATH . '/cook.txt', " ASDFASDF " . print_r($existingBuckets[$newBucketName], true) . " \n", FILE_APPEND);
			//If there is a result and its a view(view has backing_table_name set), and either its no longer written to or it has the same backing table.
			if ( $existingBuckets[$newBucketName]->backing_table_name != null 
				&& (!Bucket::isBucketWithPuts($newBucketName, $dbw) || $existingBuckets[$newBucketName]->backing_table_name == $bucketName)) {
					$needToDropView = true;
			} else {
				return wfMessage("bucket-move-already-exists", $newBucketName);
			}
		}
		return $needToDropView;
	}

	public static function moveBucket( $bucketName, $newBucketName ) {
		$dbw = MediaWikiServices::getInstance()->getDBLoadBalancer()->getConnectionRef( DB_PRIMARY );

		$bucketName = self::getValidBucketName($bucketName);
		$newBucketName = self::getValidBucketName($newBucketName);

		$tableName = "bucket__" . $bucketName;
		$newTableName = "bucket__" . $newBucketName;
		$needToDropView = Bucket::canMoveBucket($bucketName, $newBucketName);
		if ($needToDropView) { //TODO this weird multi return thing is no good. Just always try and drop it I think
			$dbw->query("DROP VIEW IF EXISTS $newTableName;");
		}
		$dbw->query("RENAME TABLE $tableName TO $newTableName;");
		$dbw->query("CREATE OR REPLACE VIEW $tableName AS SELECT * FROM $newTableName;");

		//Update bucket_schemas to have a reference to the moved table
		$existing_schema = $dbw->newSelectQueryBuilder()
			->table("bucket_schemas")
			->select("schema_json")
			->where(["table_name" => $bucketName])
			->fetchField();
		//TODO only create a view if the table isn't empty?
		//Create a new entry for the moved bucket
		$dbw->newInsertQueryBuilder()
			->insert("bucket_schemas")
			->row([
				"table_name" => $newBucketName,
				"backing_table_name" => null,
				"schema_json" => $existing_schema
			])
			->onDuplicateKeyUpdate()
			->uniqueIndexFields("table_name")
			->set([
				"backing_table_name" => null,
				"schema_json" => $existing_schema
			])
			->caller(__METHOD__)
			->execute();
		//Update the old entry, which is now representing a view, to point to new bucket
		$dbw->newUpdateQueryBuilder()
			->update("bucket_schemas")
			->set([
				"table_name" => $bucketName,
				"backing_table_name" => $newBucketName,
				"schema_json" => $existing_schema
			])
			->where(["table_name" => $bucketName])
			->caller(__METHOD__)
			->execute();
	}

	private static function getDbType( $fieldName, $fieldData ) {
		if (isset(self::$requiredColumns[$fieldName])) {
			return Bucket::$dataTypes[self::$requiredColumns[$fieldName]['type']];
		} else {
			if ( isset($fieldData['repeated']) && strlen($fieldData['repeated']) > 0) {
				return 'JSON';
			} else {
				return Bucket::$dataTypes[self::$dataTypes[$fieldData['type']]];
			}
		}
		return 'TEXT';
	}

	private static function getIndexStatement( $fieldName, $fieldData ) {
		switch ( self::getDbType( $fieldName, $fieldData ) ) {
			case 'JSON':
				$fieldData['repeated'] = false;
				$subType = self::getDbType($fieldName, $fieldData);
				switch($subType) {
					case "TEXT":
						$subType = "CHAR(255)";
						break;
					case "INTEGER":
						$subType = "DECIMAL";
						break;
					case "DOUBLE": //CAST doesn't support double 
						$subType = "CHAR(255)";
						break;
					case "BOOLEAN":
						$subType = "CHAR(255)"; //CAST doesn't have a boolean option
						break;
				}
				return "INDEX `$fieldName`((CAST(`$fieldName` AS $subType ARRAY)))";
			case 'TEXT':
			case 'PAGE':
				return "INDEX `$fieldName`(`$fieldName`(255))";
			default:
				return "INDEX `$fieldName` (`$fieldName`)";
		}
	}

	private static function getAlterTableStatement( $dbTableName, $newSchema, $oldSchema, $dbw ) {
		$alterTableFragments = [];

		unset($oldSchema["_parent_rev_id"]); // _parent_rev_id is not a column, its just metadata
		foreach ( $newSchema as $fieldName => $fieldData ) {
			#Handle new columns
			if ( !isset( $oldSchema[$fieldName] ) ) {
				$alterTableFragments[] = "ADD `$fieldName` " . self::getDbType( $fieldName, $fieldData );
				if ( $fieldData['index'] ) {
					$alterTableFragments[] = "ADD " . self::getIndexStatement($fieldName, $fieldData);
				}
			#Handle deleted columns
			} elseif ( $oldSchema[$fieldName]['index'] === true && $fieldData['index'] === false ) {
				$alterTableFragments[] = "DROP INDEX `$fieldName`";
			} else {
				#Handle type changes
				$oldDbType = self::getDbType($fieldName, $oldSchema[$fieldName]);
				$newDbType = self::getDbType($fieldName, $fieldData);
				file_put_contents(MW_INSTALL_PATH . '/cook.txt', "OLD: $oldDbType, NEW: $newDbType \n", FILE_APPEND);
				if ( $oldDbType !== $newDbType ) {
					$needNewIndex = false;
					if ( $oldSchema[$fieldName]['repeated'] || $fieldData['repeated'] 
						|| strpos(self::getIndexStatement($fieldName, $oldSchema[$fieldName]), '(') != strpos(self::getIndexStatement($fieldName, $fieldData), '(') ) {
						file_put_contents(MW_INSTALL_PATH . '/cook.txt', "DROPPING INDEX $fieldName \n", FILE_APPEND);
						#We cannot MODIFY from a column that doesn't need key length to a column that does need key length
						$alterTableFragments[] = "DROP INDEX `$fieldName`"; #Repeated types cannot reuse the existing index
						$needNewIndex = true;
					}
					if ( $oldDbType == "TEXT" && $newDbType == "JSON" ) { #Update string types to be valid JSON
						#TODO: Maybe this isn't kosher, but we need to run UPDATE before we can do ALTER
						$dbw->query("UPDATE $dbTableName SET `$fieldName` = JSON_ARRAY(`$fieldName`) WHERE NOT JSON_VALID(`$fieldName`) AND _page_id >= 0;");
					}
					$alterTableFragments[] = "MODIFY `$fieldName` " . self::getDbType($fieldName, $fieldData);
					if ( $fieldData['index'] && $needNewIndex ) {
						$alterTableFragments[] = "ADD " . self::getIndexStatement($fieldName, $fieldData);
					}
				}
				#Handle index changes
				if ( ( $oldSchema[$fieldName]['index'] === false && $fieldData['index'] === true ) ) {
					$alterTableFragments[] = "ADD " . self::getIndexStatement($fieldName, $fieldData);
				}
			}
			unset( $oldSchema[$fieldName] );
		}
		//Drop unused columns
		foreach ($oldSchema as $deletedColumn => $val) {
			#TODO performance test this
            file_put_contents(MW_INSTALL_PATH . '/cook.txt', "del column " . print_r($deletedColumn, true) . "\n", FILE_APPEND);
			$alterTableFragments[] = "DROP `$deletedColumn`";
		}

		return "ALTER TABLE $dbTableName " . implode( ', ', $alterTableFragments ) . ';';
	}

	private static function getCreateTableStatement( $dbTableName, $newSchema ) {
		$createTableFragments = [];

		foreach ( $newSchema as $fieldName => $fieldData ) {
			$dbType = self::getDbType($fieldName, $fieldData);
			$createTableFragments[] = "`$fieldName` {$dbType}";
			if ( $fieldData['index'] ) {
				$createTableFragments[] = self::getIndexStatement($fieldName, $fieldData);
			}
		}
		$createTableFragments[] = 'PRIMARY KEY (`_page_id`, `_index`)';
		return "CREATE TABLE $dbTableName (" . implode( ', ', $createTableFragments ) . ');';
	}

	public static function castToDbType( $value, $type) {
		if ( $type === 'TEXT' || $type === 'PAGE' ) {
			if ( $value == "" ) {
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
			if (!is_array($value)) {
				if ( $value == "" ) {
					return null;
				} else {
					return json_encode([$value]); //Wrap single values in an array for compatability
				}
			} else {
				//Remove empty strings
				$value = array_filter($value, function($v) { 
					return $v != ""; 
				});
				if ( count($value) > 0 ) {
					return json_encode(LuaLibrary::convertFromLuaTable($value));
				} else {
					return null;
				}
			}
		}
	}

	public static function cast( $value, $fieldData ) {
		$type = $fieldData['type'];
		if ($fieldData['repeated']) {
			$ret = [];
			$fieldData['repeated'] = false;
			$jsonData = json_decode($value, true);
			if ( !is_array($jsonData) ) { //If we are in a repeated field but only holding a scalar, make it an array anyway.
				$jsonData = [ $jsonData ];
			}
			foreach ($jsonData as $subVal) {
				$ret[] = self::cast($subVal, $fieldData);
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

	public static function sanitizeColumnName( $column, $fieldNamesToTables, $schemas, $tableName = null ) {
		if ( !is_string( $column ) ) {
			throw new QueryException( wfMessage("bucket-query-column-interpret-error", $column) );
		}
		//Category column names are specially handled
		if ( self::isCategory($column) ) {
			$tableName = "category";
			$columnName = explode(':', $column)[1];
			return [
				"fullName" => "`bucket__$tableName`.`$columnName`",
				"tableName" => $tableName,
				"columnName" => $columnName,
				"schema" => [
					"type" => "BOOLEAN",
					"index" => false,
					"repeated" => false
				]
			];
		}
		$parts = explode( '.', $column );
		if ( $column === '' || count( $parts ) > 2 ) {
			throw new QueryException( wfMessage("bucket-query-column-name-invalid", $column) );
		}
		$columnNameTemp = end( $parts );
		$columnName = self::getValidFieldName( $columnNameTemp );
		if ( !$columnName ) {
			throw new QueryException( wfMessage("bucket-query-column-name-invalid", $columnNameTemp) );
		}
		if ( count( $parts ) === 1 ) {
			if ( !isset( $fieldNamesToTables[$columnName] ) ) {
				throw new QueryException( wfMessage("bucket-query-column-not-found", $columnName ) );
			}
			if ( $tableName === null ) {
				$tableOptions = $fieldNamesToTables[$columnName];
				if ( count( $tableOptions ) > 1 ) {
					throw new QueryException( wfMessage("bucket-query-column-ambiguous", $columnName) );
				}
				$tableName = array_keys( $tableOptions )[0];
			}
		} elseif ( count( $parts ) === 2 ) {
			$columnTableName = self::getValidFieldName( $parts[0] );
			if ( !$columnTableName ) {
				throw new QueryException( wfMessage("bucket-invalid-name-warning", $parts[0]) );
			}
			if ( $tableName !== null && $columnTableName !== $tableName ) {
				throw new QueryException( wfMessage("bucket-query-bucket-invalid", $parts[0]) );
			}
			$tableName = $columnTableName;
		}
		if ( !isset( $schemas[$tableName] ) ) {
			throw new QueryException( wfMessage("bucket-query-bucket-not-found", $tableName) );
		}
		if ( !isset( $schemas[$tableName][$columnName] ) ) {
			throw new QueryException( wfMessage("bucket-query-column-not-found-in-bucket", $columnName,  $tableName) );
		}
		return [
			"fullName" => "`bucket__$tableName`.`$columnName`",
			"tableName" => $tableName,
			"columnName" => $columnName,
			"schema" => $schemas[$tableName][$columnName]
		];
	}

	public static function isNot( $condition ) {
		return is_array( $condition )
		&& isset($condition['op'])
		&& $condition['op'] == 'NOT'
		&& isset( $condition['operand'] );
	}

	public static function isOrAnd( $condition ) {
		return is_array( $condition )
		&& isset($condition['op'])
		&& ( $condition['op'] === 'OR' || $condition['op'] === 'AND' )
		&& isset( $condition['operands'])
		&& is_array( $condition['operands'] );
	}

	public static function isCategory( $columnName ) {
		return substr(strtolower(trim($columnName)), 0, 9) == "category:";
	}

	/**
	 *  $condition is an array of members:
	 * 		operands -> Array of $conditions
	 * 		(optional)op -> AND | OR | NOT
	 * 		unnamed -> scalar value or array of scalar values
	 */
	public static function getWhereCondition( $condition, $fieldNamesToTables, $schemas, $dbw, &$categoryJoins ) {
		// file_put_contents(MW_INSTALL_PATH . '/cook.txt', "Condition: " . print_r($condition, true) . "\n", FILE_APPEND);
		if ( self::isOrAnd( $condition ) ) {
			if ( empty( $condition['operands'] ) ) {
				throw new QueryException( wfMessage("bucket-query-where-missing-cond", json_encode( $condition )) );
			}
			$children = [];
			foreach ( $condition['operands'] as $key => $operand ) {
				if ($key != 'op') { //the key 'op' will never be a valid condition on its own.
					if (!isset($operand['op']) && isset($condition['op']) && isset($operand[0]) && is_array($operand[0]) && count($operand[0]) > 0) {
						$operand['op'] = $condition['op']; //Set child op to parent
					}
					// file_put_contents(MW_INSTALL_PATH . '/cook.txt', "Calling getWhereCondition 1: " . print_r($operand, true) . "\n", FILE_APPEND);
					$children[] = self::getWhereCondition($operand, $fieldNamesToTables, $schemas, $dbw, $categoryJoins );
				}
			}
			$children = implode( " {$condition['op']} ", $children );
			return "($children)";
		} elseif ( self::isNot( $condition ) ) {
			// file_put_contents(MW_INSTALL_PATH . '/cook.txt', "Calling getWhereCondition for NOT: " . print_r($condition, true) . "\n", FILE_APPEND);
			$child = self::getWhereCondition( $condition['operand'], $fieldNamesToTables, $schemas, $dbw, $categoryJoins );
			return "(NOT $child)";
		} elseif ( is_array( $condition ) && is_array( $condition[0] ) ) {
			// .where{{"a", ">", 0}, {"b", "=", "5"}})
			// file_put_contents(MW_INSTALL_PATH . '/cook.txt', "Calling getWhereCondition 2 with overriding op: " . print_r($condition, true) . "\n", FILE_APPEND);
			return self::getWhereCondition( [ 'op' => isset($condition[ 'op' ]) ? $condition[ 'op' ] : 'AND', 'operands' => $condition ], $fieldNamesToTables, $schemas, $dbw, $categoryJoins );
		} elseif ( is_array( $condition ) && !empty( $condition ) && !isset( $condition[0] ) ) {
			// .where({a = 1, b = 2})
			$operands = [];
			foreach ( $condition as $key => $value ) {
				$operands[] = [ $key, '=', $value ];
			}
			// file_put_contents(MW_INSTALL_PATH . '/cook.txt', "Calling getWhereCondition 3 with overriding op = AND: " . print_r($operands, true) . "\n", FILE_APPEND);
			return self::getWhereCondition( [ 'op' => 'AND', 'operands' => $operands ], $fieldNamesToTables, $schemas, $dbw, $categoryJoins );
		} elseif ( is_array( $condition ) && isset( $condition[0] ) && isset( $condition[1] ) ) {
			if ( count( $condition ) === 2 ) {
				$condition = [ $condition[0], '=', $condition[1] ];
			}
			$columnNameData = self::sanitizeColumnName($condition[0], $fieldNamesToTables, $schemas);
			if (!isset(self::$WHERE_OPS[$condition[1]])) {
				throw new QueryException(wfMessage("bucket-query-where-invalid-op", $condition[1]));
			}
			$op = $condition[1];
			$value = $condition[2];

			$columnName = $columnNameData["fullName"];
			// file_put_contents(MW_INSTALL_PATH . '/cook.txt', "DATA " . print_r($fieldNamesToTables, true) . "\n", FILE_APPEND);
			// file_put_contents(MW_INSTALL_PATH . '/cook.txt', "COLUMNS " . print_r($columnNameData, true) . "\n", FILE_APPEND);
			if ( $value == "&&NULL&&" ) {
				//TODO if op is something other than equals throw warning?
				return "($columnName IS NULL)";
			} elseif ($fieldNamesToTables[$columnNameData["columnName"]][$columnNameData["tableName"]]["repeated"] == true) {
				return "\"$value\" MEMBER OF($columnName)";
			} else {
				if (is_numeric($value)) {
					return "($columnName $op $value)";
				} elseif (is_string($value)) {
					// TODO: really don't like this
					$value = $dbw->strencode($value);
					return "($columnName $op \"$value\")";
				}
			}
		} elseif ( is_string($condition) && self::isCategory( $condition ) || (is_array($condition) && self::isCategory($condition[0]))) {
			if (is_array($condition)) {
				$condition = $condition[0];
			}
			$categoryName = explode(':', $condition)[1];
			$categoryJoins[$categoryName] = $condition;	
			return "(`$condition`.cl_to IS NOT NULL)";
		}
		throw new QueryException( wfMessage("bucket-query-where-confused", json_encode( $condition ) ) );
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

		$primaryTableName = self::getValidFieldName( $data['tableName'] );
		if ( !$primaryTableName ) {
			throw new QueryException( wfMessage("bucket-invalid-name-warning", $data['tableName']) );
		}
		$tableNames[ $primaryTableName ] = true;
		$TABLES['bucket__' . $primaryTableName] = 'bucket__' . $primaryTableName;

		foreach ( $data['joins'] as $join ) {
			$tableName = self::getValidFieldName( $join['tableName'] );
			if ( !$tableName ) {
				throw new QueryException( wfMessage("bucket-invalid-name-warning", $join['tableName']) );
			}
			if ( isset($tableNames[$tableName]) ) {
				throw new QueryException( wfMessage("bucket-select-duplicate-join", $tableName) );
			}
			$tableNames[$tableName] = true;
			$TABLES['bucket__' . $tableName] = 'bucket__' . $tableName;
			$join['tableName'] = $tableName;
		}

		$tableNamesList = array_keys( $tableNames );
		foreach ( $tableNames as $tableName => $val ) {
			if ( isset(self::$allSchemas[$tableName]) && self::$allSchemas[$tableName] ) {
				unset( $tableNames[$tableName] );
			}
		}

		$dbw = MediaWikiServices::getInstance()->getDBLoadBalancer()->getConnectionRef( DB_PRIMARY );
		$missingTableNames = array_keys( $tableNames );
		if ( !empty( $missingTableNames ) ) {
			$res = $dbw->select( 'bucket_schemas', [ 'table_name', 'schema_json' ], [ 'table_name' => $missingTableNames ] );

			$schemas = [];
			foreach ( $res as $row ) {
				self::$allSchemas[$row->table_name] = json_decode( $row->schema_json, true );
			}
		}
		foreach ( $tableNamesList as $tableName ) {
			if ( !array_key_exists($tableName, self::$allSchemas) || !self::$allSchemas[$tableName] ) {
				throw new QueryException( wfMessage("bucket-no-exist", $tableName) );
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
			if ( self::isCategory($selectColumn) ) {
				$SELECTS[$selectColumn] = "{$dbw->addIdentifierQuotes($selectColumn)}.`cl_to` IS NOT NULL";
				$categoryName = explode(':', $selectColumn)[1];
				$categoryJoins[$categoryName] = $selectColumn;
				continue;
			} else {
				// TODO: don't like this
				$selectColumn = strtolower(trim($selectColumn));
				$selectTableName = null;
				//If we don't have a period then we must be the primary column.
				if (count(explode('.', $selectColumn)) == 1) {
					$selectTableName = $primaryTableName;
				}

				$colData = self::sanitizeColumnName($selectColumn, $fieldNamesToTables, $schemas, $selectTableName);

				if ($colData['tableName'] != $primaryTableName) {
					$SELECTS[$selectColumn] = 'JSON_ARRAY(' . $colData["fullName"] . ')';
				} else {
					$SELECTS[$selectColumn] = $colData["fullName"];
				}
			}
			$ungroupedColumns[$dbw->addIdentifierQuotes($selectColumn)] = true;
		}

		if ( !empty( $data['wheres']['operands'] ) ) {
			$WHERES[] = self::getWhereCondition( $data['wheres'], $fieldNamesToTables, $schemas, $dbw, $categoryJoins );
		}

		if ( !empty( $categoryJoins ) ) {

			foreach ( $categoryJoins as $categoryName => $alias ) {
				$TABLES[$alias] = 'categorylinks';
				$LEFT_JOINS[$alias] = [
					"`$alias`.cl_from = `bucket__$primaryTableName`.`_page_id`",//Must be all in one string to avoid the table name being treated as a string value.
					"`$alias`.cl_to" => str_replace(" ", "_", $categoryName)
				];
			}
		}

		foreach ( $data['joins'] as $join ) {
			if ( !is_array($join["cond"]) || !count($join["cond"]) == 2) {
				throw new QueryException( wfMessage("bucket-query-invalid-join", json_encode( $join )));
			}
			$leftField = self::sanitizeColumnName( $join['cond'][0], $fieldNamesToTables, $schemas );
			$isLeftRepeated = $leftField["schema"]["repeated"];
			$rightField = self::sanitizeColumnName( $join['cond'][1], $fieldNamesToTables, $schemas );
			$isRightRepeated = $rightField["schema"]["repeated"];
			

			if ($isLeftRepeated && $isRightRepeated) {
				throw new QueryException( wfMessage("bucket-invalid-join-two-repeated", $leftField["fullName"], $rightField["fullName"]));
			}

			if ($isLeftRepeated || $isRightRepeated) {
				//Make the left field the repeated one just for consistency.
				if ( $isRightRepeated ) {
					$tmp = $leftField;
					$isTmp = $isLeftRepeated;
					$leftField = $rightField;
					$isLeftRepeated = $isRightRepeated;
					$rightField = $tmp;
					$isRightRepeated = $isTmp;
				}

				$LEFT_JOINS['bucket__' . $join['tableName']] = [
					"{$rightField['fullName']} MEMBER OF({$leftField['fullName']})"
				];
			} else {
				$LEFT_JOINS['bucket__' . $join['tableName']] = [
					"{$leftField['fullName']} = {$rightField['fullName']}"
				];
			}
		}

		$OPTIONS['GROUP BY'] = array_keys( $ungroupedColumns );

		$OPTIONS['LIMIT'] = Bucket::DEFAULT_LIMIT;
		if ( isset($data['limit']) && is_int( $data['limit'] ) && $data['limit'] >= 0 ) {
			$OPTIONS['LIMIT'] = min( $data['limit'], Bucket::MAX_LIMIT );
		}

		$OPTIONS['OFFSET'] = 0;
		if ( isset($data['offset']) && is_int( $data['offset'] ) && $data['offset'] >= 0 ) {
			$OPTIONS['OFFSET'] = $data['offset'];
		}

		$rows = [];
		// file_put_contents(MW_INSTALL_PATH . '/cook.txt', "TABLES " . print_r($TABLES, true) . "\n", FILE_APPEND);
		// file_put_contents(MW_INSTALL_PATH . '/cook.txt', "SELECTS " . print_r($SELECTS, true) . "\n", FILE_APPEND);
		// file_put_contents(MW_INSTALL_PATH . '/cook.txt', "WHERES " . print_r($WHERES, true) . "\n", FILE_APPEND);
		// file_put_contents(MW_INSTALL_PATH . '/cook.txt', "OPTIONS " . print_r($OPTIONS, true) . "\n", FILE_APPEND);
		// file_put_contents(MW_INSTALL_PATH . '/cook.txt', "LEFT_JOINS " . print_r($LEFT_JOINS, true) . "\n", FILE_APPEND);
		$tmp = $dbw->newSelectQueryBuilder()
			->from('bucket__' . $primaryTableName)
			->select($SELECTS)
			->where($WHERES)
			->options($OPTIONS)
			->caller( __METHOD__ )
			->setMaxExecutionTime(500);
		//TODO should probably be all in a single join call? IDK.
		foreach ($LEFT_JOINS as $alias => $conds) {
			$tmp->leftJoin($TABLES[$alias], $alias, $conds);
		}
		if ( isset($data['orderBy']) ) {
			$orderName = self::sanitizeColumnName($data['orderBy']['fieldName'], $fieldNamesToTables, $schemas)["fullName"];
			if ( $orderName != false ) {
				$tmp->orderBy($orderName, $data['orderBy']['direction']);
			} else {
				//TODO throw warning
			}
		}
		// file_put_contents(MW_INSTALL_PATH . '/cook.txt', "SQL " . print_r($tmp->getSQL(), true) . "\n", FILE_APPEND);
		$res = $tmp->fetchResultSet();
		foreach ( $res as $row ) {
			// file_put_contents(MW_INSTALL_PATH . '/cook.txt', "ROWS " . print_r($row, true) . "\n", FILE_APPEND);
			$row = (array)$row;
			foreach ( $row as $columnName => $value ) {
				$defaultTableName = null;
				//If we don't have a period in the column name it must be a primary table column.
				if (count(explode( '.', $columnName )) == 1) {
					$defaultTableName = $primaryTableName;
				}
				$schema = self::sanitizeColumnName($columnName, $fieldNamesToTables, $schemas, $defaultTableName)["schema"];
				$row[$columnName] = self::cast($value, $schema);
			}
			$rows[] = $row;
		}
		return $rows;
	}
}

//TODO all schema exception strings need to be translation strings
class SchemaException extends LogicException {
	function __construct($msg)
	{
		file_put_contents( MW_INSTALL_PATH . '/cook.txt', "SCHEMA EXCEPTION " . print_r($msg, true) . "\n" , FILE_APPEND);
		parent::__construct($msg);
	}
}

//TODO all queryException strings need to not have periods
class QueryException extends LogicException {
	function __construct($msg)
	{
		file_put_contents( MW_INSTALL_PATH . '/cook.txt', "QUERY EXCEPTION " . print_r($msg, true) . "\n" , FILE_APPEND);
		parent::__construct($msg);
	}
}
