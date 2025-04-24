<?php

namespace MediaWiki\Extension\Bucket;

use MediaWiki\MediaWikiServices;
use Exception;

class Bucket {
	public const EXTENSION_DATA_KEY = 'bucket:puts';
	public const EXTENSION_PROPERTY_KEY = 'bucketputs';

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
			'page_name' => [ 'type' => 'TEXT', 'index' => true,  'repeated' => false ],
			'page_name_version' => [ 'type' => 'TEXT', 'index' => true , 'repeated' => false ]
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

	private static $MAX_LIMIT = 5000;
	private static $DEFAULT_LIMIT = 500;

	#Compare equality of two arrays
	private static function areEqual( $array1, $array2 ) {
		if (count($array1) != count($array2)) {
			return false;
		}
		foreach ( $array1 as $key => $value ) {
			if ( !array_key_exists( $key, $array2) ) {
				return false;
			}
			if ( $value != $array2[$key] ) {
				#We are comparing a value from the database with a value from PHP.
				#The database text json has a space after a comma in the array, PHP doesn't.
				#So decode the mismatched values and check for equality in the data they represent.
				if ( json_decode($value) == json_decode($array2[$key])) {
					continue;
				}
				return false;
			}
		}
		return true;
	}

	/*
	Called when a page is saved containing a bucket.put
	*/
	public static function writePuts( int $pageId, string $titleText, array $puts ) {
		// file_put_contents( MW_INSTALL_PATH . '/cook.txt', "writePuts start " . print_r($puts, true) . "\n" , FILE_APPEND);
		$dbw = MediaWikiServices::getInstance()->getDBLoadBalancer()->getConnectionRef( DB_PRIMARY );

		$res = $dbw->select( 'bucket_schemas', [ 'table_name', 'schema_json' ], [ 'table_name' => array_keys( $puts ) ] );

		$schemas = [];
		foreach ( $res as $row ) {
			$schemas[$row->table_name] = json_decode( $row->schema_json, true );
		}

		$bucket_hash = [];
		#TODO is specifying more constraints better or worse?
		$res = $dbw->select( 'bucket_pages', '*', [ 'page_id' => $pageId ]);
		foreach ( $res as $row ) {
			$bucket_hash[$row->table_name] = $row->put_hash;
		}

		foreach ( $puts as $tableName => $tablePuts ) {
			// TODO: this misses deleting things that are no longer in the output
			// TODO: not safe
			$dbTableName = 'bucket__' . $tableName;
			$res = $dbw->select(
				$dbTableName,
				'*',
				[ '_page_id' => $pageId ]
			);

			$fields = [];
			$fieldNames = $res->getFieldNames();
			foreach ( $fieldNames as $fieldName ) {
				// TODO: match on type, not just existence
				$fields[ $fieldName ] = true;
			}
			foreach ( $tablePuts as $idx => $singlePut ) {
				$singlePut['_page_id'] = $pageId;
				$singlePut['_index'] = $idx;
				$singlePut['page_name'] = $titleText;
				$singlePut['page_name_version'] = $titleText;
				if ( isset( $singlePut['_version'] ) ) {
					$singlePut['page_name_version'] = $titleText . '#' . $singlePut['_version'];
				}
				foreach ( $singlePut as $key => $value ) {
					if ( !isset($fields[$key]) || !$fields[$key] ) {
						// TODO: warning somewhere?
						file_put_contents( MW_INSTALL_PATH . '/cook.txt', "writePuts KEY UNSET " . print_r($key, true) . "\n" , FILE_APPEND);
						unset( $singlePut[$key] );
					} else {
						#TODO JSON relies on forcing utf8 transmission in DatabaseMySQL.php line 829
						$singlePut[$key] = self::castToDbType( $value, self::getDbType( $fieldName, $schemas[$tableName][$key] ) );
					}
				}
				$tablePuts[$idx] = $singlePut;
			}

			#Check these puts against the hash of the last time we did puts.
			$newHash = hash( 'sha256', json_encode( $tablePuts ) );
			if ( isset($bucket_hash[ $tableName ]) && $bucket_hash[ $tableName ] == $newHash ) {
				file_put_contents(MW_INSTALL_PATH . '/cook.txt', "HASH MATCH SKIPPING WRITING =====================\n", FILE_APPEND);
				continue;
			}

			#TODO is it better to not read existing data and just write it all?
			$existingRows = [];
			foreach ( $res as $row ) {
				$existingRows[$row->_index] = (array) $row;
			} 

			$newPuts = [];
			foreach ( $tablePuts as $put ) {
				$idx = $put[ '_index' ];
				// if ( !key_exists($idx, $existingRows) || !self::areEqual( $put, $existingRows[ $idx ] )) {
					$newPuts[] = $put;
				// }
			}

			$newDeletes = [];
			foreach ( $existingRows as $existing ) {
				$idx = $existing[ '_index' ];
				// if ( !key_exists($idx, $tablePuts) || !self::areEqual( $existing, $tablePuts[ $idx ] ) ) {
					$newDeletes[] = $existing['_index'];
				// }
			}
			

			file_put_contents( MW_INSTALL_PATH . '/cook.txt', "writePuts puts " . print_r($newPuts, true) . "\n" , FILE_APPEND);
			file_put_contents( MW_INSTALL_PATH . '/cook.txt', "writePuts deletes " . print_r($newDeletes, true) . "\n" , FILE_APPEND);
			// TODO: does behavior here depend on DBO_TRX?
			$dbw->begin();
			if ( !empty($newDeletes) ) {
				$dbw->newDeleteQueryBuilder()
					->deleteFrom( $dbTableName )
					->where( [ '_page_id' => $pageId, '_index' => $newDeletes ] )
					->execute();
				// TODO: maybe chunk?
			}
			$dbw->insert( $dbTableName, $newPuts );
			#TODO we need to delete entries from bucket_pages if a bucket is removed from a page
			#We can put used buckets in a list and then compare with all buckets assigned to this page and then delete the difference at the end.
			$dbw->delete( 'bucket_pages', ['page_id' => $pageId, 'table_name' => $tableName ] );
			$dbw->insert( 'bucket_pages', ['page_id' => $pageId, 'table_name' => $tableName, 'put_hash' => $newHash ] );

			$dbw->commit();
			file_put_contents( MW_INSTALL_PATH . '/cook.txt', "commited\n", FILE_APPEND );
		}
	}

	public static function getValidFieldName( string $fieldName ) {
		if ( preg_match( '/^[a-zA-Z0-9_]+$/', $fieldName ) ) {
			return strtolower( trim( $fieldName ) );
		}
		return false;
	}

	public static function createOrModifyTable( string $bucketName, object $jsonSchema ) {
		$newSchema = array_merge( [], self::$requiredColumns );

		if ( empty( (array)$jsonSchema ) ) {
			throw new SchemaException( 'Need at least one column in the schema.' );
		}

		$bucketName = self::getValidFieldName( $bucketName );
		if ( !$bucketName ) {
			throw new SchemaException( 'Bucket name not valid.' );
		}

		foreach ( $jsonSchema as $fieldName => $fieldData ) {
			if ( gettype( $fieldName ) !== 'string' ) {
				throw new SchemaException( 'All field names must be strings: ' . $fieldName );
			}

			$lcFieldName = self::getValidFieldName( $fieldName );
			if ( !$lcFieldName ) {
				throw new SchemaException( 'Invalid field name: ' . $fieldName );
			}

			$lcFieldName = strtolower( $fieldName );
			if ( isset( $newSchema[$lcFieldName] ) ) {
				throw new SchemaException( 'Duplicate field name: ' . $fieldName );
			}

			if ( !isset( self::$dataTypes[$fieldData->type] ) ) {
				throw new SchemaException( 'Invalid data type for field ' . $fieldName . ': ' . $fieldData->type );
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
		if ( !$oldSchema ) {
			$statement = self::getCreateTableStatement( $dbTableName, $newSchema );
			file_put_contents(MW_INSTALL_PATH . '/cook.txt', "CREATE TABLE STATEMENT $statement \n", FILE_APPEND);
			$dbw->query( $statement );
		} else {
			$oldSchema = json_decode( $oldSchema, true );
			$statement = self::getAlterTableStatement( $dbTableName, $newSchema, $oldSchema );
			file_put_contents(MW_INSTALL_PATH . '/cook.txt', "ALTER TABLE STATEMENT $statement \n", FILE_APPEND);
			$dbw->query( $statement );

		}
		$schemaJson = json_encode( $newSchema );
		$dbw->upsert(
			'bucket_schemas',
			[ 'table_name' => $bucketName, 'schema_json' => $schemaJson ],
			'table_name',
			[ 'schema_json' => $schemaJson ]
		);
	}

	private static function getDbType( $fieldName, $fieldData ) {
		if (isset(self::$requiredColumns[$fieldName])) {
			return self::$requiredColumns[$fieldName]['type'];
		} else {
			if ( $fieldData['repeated'] ) {
				return 'JSON';
			} else {
				return self::$dataTypes[$fieldData['type']];
			}
		}
		return 'TEXT';
	}

	private static function getIndexStatement( $fieldName, $fieldData ) {
		switch ( self::getDbType( $fieldName, $fieldData ) ) {
			case 'JSON':
				return "INDEX `$fieldName`((CAST(`$fieldName` AS CHAR(255) ARRAY)))"; #TODO actually cast this to the right underlying type
			case 'TEXT':
			case 'PAGE':
				return "INDEX `$fieldName`(`$fieldName`(255))";
			default:
				return "INDEX `$fieldName` (`$fieldName`)";
		}
	}

	private static function getAlterTableStatement( $dbTableName, $newSchema, $oldSchema ) {
		#TODO handle repeated
		// Note that we do not support actually removing a column from the DB,
		// or changing the type (all user-defined columns are TEXT) #TODO this is not true
		// only support is for ADD COLUMN, ADD INDEX, DROP INDEX
		$alterTableFragments = [];

		#TODO index everything?
		foreach ( $newSchema as $fieldName => $fieldData ) {

			#Handle new columns
			if ( !isset( $oldSchema[$fieldName] ) ) {
				$alterTableFragments[] = "ADD `$fieldName` " . self::getDbType( $fieldName, $fieldData );
				if ( $fieldData['index'] ) {
					$alterTableFragments[] = "ADD " . self::getIndexStatement($fieldName, $fieldData);
				}
			#Handle deletedcolumns
			} elseif ( $oldSchema[$fieldName]['index'] === true && $fieldData['index'] === false ) {
				$alterTableFragments[] = "DROP INDEX `$fieldName`";
			} else {
				#Handle type changes
				$newDbType = self::getDbType($fieldName, $fieldData);
				if ( self::getDbType($fieldName, $oldSchema[$fieldName]) !== $newDbType ) {
					// Do data type conversions
					#TODO make this all one transaction
					$dbw = MediaWikiServices::getInstance()->getDBLoadBalancer()->getConnectionRef(DB_PRIMARY);
					$columnNameTemp = $fieldName . '__tmp';
					$query = "ALTER TABLE `$dbTableName` ADD `$columnNameTemp` $newDbType ";
					if ($fieldData['index'] == true) {
						$query = $query . ", ADD INDEX `$columnNameTemp` (`$columnNameTemp`)";// . self::getIndexStatement($fieldName, $fieldData);
					}
					file_put_contents(MW_INSTALL_PATH . '/cook.txt', "ALTER TYPE $query \n", FILE_APPEND);
					$dbw->query($query . ";"); //Make new temp column
					$dbw->query("UPDATE IGNORE `$dbTableName` SET `$columnNameTemp` = $fieldName WHERE `_page_id` >= 0;");
					$dbw->query("ALTER TABLE `$dbTableName` DROP COLUMN `$fieldName`, RENAME COLUMN `$columnNameTemp` TO `$fieldName`;");
				}
				#Handle index changes
				if ( ( $oldSchema[$fieldName]['index'] === false && $fieldData['index'] === true ) ) {
					$alterTableFragments[] = "ADD " . self::getIndexStatement($fieldName, $fieldData);
				}
			}
			unset( $oldSchema[$fieldName] );
		}

		// For any removed columns, we can at least drop indexes
		foreach ( $oldSchema as $fieldName => $fieldData ) {
			if ( $fieldData['index'] ) {
				$alterTableFragments[] = "DROP INDEX `$fieldName`";
			}
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
			return $value;
		} elseif ( $type === 'DOUBLE' ) {
			return floatval( $value );
		} elseif ( $type === 'INTEGER' ) {
			return intval( $value );
		} elseif ( $type === 'BOOLEAN' ) {
			return boolval( $value );
		} elseif ( $type === 'JSON' ) {
			return json_encode( LuaLibrary::convertFromLuaTable($value) );
		}
	}

	public static function cast( $value, $type ) {
		if ( $type === 'TEXT' || $type === 'PAGE' ) {
			return $value;
		} elseif ( $type === 'DOUBLE' ) {
			return floatval( $value );
		} elseif ( $type === 'INTEGER' ) {
			return intval( $value );
		} elseif ( $type === 'BOOLEAN' ) {
			return boolval( $value );
		} elseif ( $type === 'JSON' ) {
			return json_decode( $value, true );
		}
	}

	public static function sanitizeColumnName( $column, $fieldNamesToTables, $schemas, $tableName = null ) {
		if ( !is_string( $column ) ) {
			throw new QueryException( "Can't interpret column: $column" );
		}
		$parts = explode( '.', $column );
		if ( $column === '' || count( $parts ) > 2 ) {
			throw new QueryException( "Invalid column name: $column" );
		}
		$columnNameTemp = end( $parts );
		$columnName = self::getValidFieldName( $columnNameTemp );
		if ( !$columnName ) {
			throw new QueryException( "Invalid column name: $columnNameTemp." );
		}
		if ( count( $parts ) === 1 ) {
			if ( !isset( $fieldNamesToTables[$columnName] ) ) {
				throw new QueryException( "Column name $columnName not found." );
			}
			if ( $tableName === null ) {
				$tableOptions = $fieldNamesToTables[$columnName];
				if ( count( $tableOptions ) > 1 ) {
					throw new QueryException( "Column name $columnName is ambiguous." );
				}
				$tableName = array_keys( $tableOptions )[0];
			}
		} elseif ( count( $parts ) === 2 ) {
			$columnTableName = self::getValidFieldName( $parts[0] );
			if ( !$columnTableName ) {
				throw new QueryException( "Invalid bucket name: {$parts[0]}" );
			}
			if ( $tableName !== null && $columnTableName !== $tableName ) {
				throw new QueryException( "Can't use bucket name {$parts[0]} here." );
			}
			$tableName = $columnTableName;
		}
		if ( !isset( $schemas[$tableName] ) ) {
			throw new QueryException( "Bucket name $tableName not found in query." );
		}
		if ( !isset( $schemas[$tableName][$columnName] ) ) {
			throw new QueryException( "Column $columnName not found in bucket $tableName." );
		}
		return "`bucket__$tableName`.`$columnName`";
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
		&& is_array( $condition['operands'] );
	}

	public static function getCategoriesCondition( $condition, &$categoryMap ) {
		if ( self::isOrAnd( $condition ) ) {
			if ( empty( $condition['operands'] ) ) {
				throw new QueryException( 'Missing condition: ' . json_encode( $condition ) );
			}
			$children = [];
			foreach ( $condition['operands'] as $operand ) {
				$children[] = self::getCategoriesCondition( $operand, $categoryMap );
			}
			$children = implode( " {$condition['op']} ", $children );
			return "($children)";
		} elseif ( self::isNot( $condition ) ) {
			$child = self::getCategoriesCondition( $condition['operand'], $categoryMap );
			return "(NOT $child)";
		} elseif ( is_string( $condition ) ) {
			// TODO: better way to do this?
			// TODO: strencode elsewhere
			$category = ucfirst( str_replace( ' ', '_', $condition ) );
			if ( !isset( $categoryMap[$category] ) ) {
				$categoryMap[$category] = 'categorylinks_' . count( $categoryMap );
			}
			$alias = $categoryMap[$category];
			return "($alias.cl_to IS NOT NULL)";
		}
		throw new QueryException( 'Did not understand category condition: ' . json_encode( $condition ) );
	}

	public static function getWhereCondition( $condition, $fieldNamesToTables, $schemas, $dbw ) {
		if ( self::isOrAnd( $condition ) ) {
			if ( empty( $condition['operands'] ) ) {
				throw new QueryException( 'Missing condition: ' . json_encode( $condition ) );
			}
			$children = [];
			foreach ( $condition['operands'] as $operand ) {
				$children[] = self::getWhereCondition( $operand, $fieldNamesToTables, $schemas, $dbw );
			}
			$children = implode( " {$condition['op']} ", $children );
			return "($children)";
		} elseif ( self::isNot( $condition ) ) {
			$child = self::getWhereCondition( $condition['operand'], $fieldNamesToTables, $schemas, $dbw );
			return "(NOT $child)";
		} elseif ( is_array( $condition ) && is_array( $condition[0] ) ) {
			// .where{{"a", ">", 0}, {"b", "=", "5"}})
			return self::getWhereCondition( [ 'op' => 'AND', 'operands' => $condition ], $fieldNamesToTables, $schemas, $dbw );
		} elseif ( is_array( $condition ) && !empty( $condition ) && !isset( $condition[0] ) ) {
			// .where({a = 1, b = 2})
			$operands = [];
			foreach ( $condition as $key => $value ) {
				$operands[] = [ $key, '=', $value ];
			}
			return self::getWhereCondition( [ 'op' => 'AND', 'operands' => $operands ], $fieldNamesToTables, $schemas, $dbw );
		} elseif ( is_array( $condition ) && isset( $condition[0] ) && isset( $condition[1] ) ) {
			if ( count( $condition ) === 2 ) {
				$condition = [ $condition[0], '=', $condition[1] ];
			}
			$columnName = self::sanitizeColumnName( $condition[0], $fieldNamesToTables, $schemas );
			if ( !isset( self::$WHERE_OPS[$condition[1]] ) ) {
				throw new QueryException( 'Invalid op for WHERE: ' . $condition[1] );
			}
			$op = $condition[1];
			$value = $condition[2];
			if ( is_numeric( $value ) ) {
				return "($columnName $op $value)";
			} elseif ( is_string( $value ) ) {
				// TODO: really don't like this
				$value = $dbw->strencode( $value );
				return "($columnName $op \"$value\")";
			}
		}
		throw new QueryException( 'Did not understand where condition: ' . json_encode( $condition ) );
	}

	public static function runSelect( $data ) {
		$SELECTS = [];
		$JOINS = [];
		$TABLES = [];
		$WHERES = [];
		$OPTIONS = [];
		// check to see if any duplicates
		$tableNames = [];

		$primaryTableName = self::getValidFieldName( $data['tableName'] );
		if ( !$primaryTableName ) {
			throw new QueryException( "Bucket name {$data['tableName']} is not valid." );
		}
		$tableNames[ $primaryTableName ] = true;
		$TABLES['bucket__' . $primaryTableName] = 'bucket__' . $primaryTableName;

		foreach ( $data['joins'] as $join ) {
			$tableName = self::getValidFieldName( $join['tableName'] );
			if ( !$tableName ) {
				throw new QueryException( "Bucket name {$join['tableName']} is not valid." );
			}
			if ( $tableNames[$tableName] ) {
				throw new QueryException( "Bucket $tableName is already used and can't be JOINed again." );
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
			if ( !self::$allSchemas[$tableName] ) {
				throw new QueryException( "Bucket $tableName does not exist." );
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
					$fieldNamesToTables[$fieldName][$tableName] = $fieldData['type'];
				}
			}
		}

		$ungroupedColumns = [];
		foreach ( $data['selects'] as $selectColumn ) {
			// TODO: don't like this
			$selectColumn = strtolower( trim( $selectColumn ) );
			$SELECTS[$selectColumn] = self::sanitizeColumnName( $selectColumn, $fieldNamesToTables, $schemas, $primaryTableName );
			$ungroupedColumns[$selectColumn] = true;
		}

		if ( !empty( $data['categories']['operands'] ) ) {
			$categoryMap = [];
			$categoriesCondition = self::getCategoriesCondition( $data['categories'], $categoryMap );

			foreach ( $categoryMap as $categoryName => $alias ) {
				$TABLES[$alias] = 'categorylinks';
				$JOINS[$alias] = [ 'LEFT JOIN', [
					"`$alias`.cl_from" => "`bucket__$primaryTableName`._page_id",
					"`$alias`.cl_to" => $categoryName
				] ];
			}
			$WHERES[] = $categoriesCondition;
		}

		if ( !empty( $data['wheres']['operands'] ) ) {
			$WHERES[] = self::getWhereCondition( $data['wheres'], $fieldNamesToTables, $schemas, $dbw );
		}

		foreach ( $data['joins'] as $join ) {
			if ( !is_string( $join['fieldName'] ) || !is_array( $join['selectFields'] ) ) {
				throw new QueryException( 'Invalid join: ' . json_encode( $join ) );
			}
			$fieldName = self::sanitizeColumnName( $join['fieldName'], $fieldNamesToTables, $schemas );

			$jsonObjectFragments = [];
			foreach ( $join['selectFields'] as $joinSelectColumn ) {
				// TODO: don't like this
				$joinSelectColumn = strtolower( trim( $joinSelectColumn ) );
				$joinSelectValue = self::sanitizeColumnName( $joinSelectColumn, $fieldNamesToTables, $schemas, $join['tableName'] );
				$jsonObjectFragments[] = "\"$joinSelectColumn\", $joinSelectValue";

			}

			$jsonObject = 'JSON_ARRAYAGG(JSON_OBJECT(' . implode( ', ', $jsonObjectFragments ) . '))';
			$SELECTS[$join['tableName']] = $jsonObject;
			$JOINS['bucket__' . $join['tableName']] = [ 'LEFT JOIN', [
				"`bucket__{$join['tableName']}`.page_name = $fieldName"
			] ];
		}

		$OPTIONS['GROUP BY'] = array_keys( $ungroupedColumns );

		$OPTIONS['LIMIT'] = self::$DEFAULT_LIMIT;
		if ( isset($data['limit']) && is_int( $data['limit'] ) && $data['limit'] >= 0 ) {
			$OPTIONS['LIMIT'] = min( $data['limit'], self::$MAX_LIMIT );
		}

		$rows = [];
		$res = $dbw->select( $TABLES, $SELECTS, $WHERES, '', $OPTIONS, $JOINS );
		foreach ( $res as $row ) {
			$row = (array)$row;
			foreach ( $row as $columnName => $value ) {
				if ( $ungroupedColumns[$columnName] ) {
					$row[$columnName] = self::cast( $value, $schemas[$primaryTableName][$columnName]['type'] );
				} else {
					$value = json_decode( $value, true );
					$processed = [];
					foreach ( $value as $jsonRow ) {
						foreach ( $jsonRow as $jsonColumnName => $jsonValue ) {
							$jsonRow[$jsonColumnName] = self::cast( $jsonValue, $schemas[$columnName][$jsonColumnName]['type'] );
						}
						$processed[] = $jsonRow;
					}
					if ( count( $processed ) === 0 ) {
						$row[$columnName] = null;
					} elseif ( count( $processed ) === 1 ) {
						$row[$columnName] = $processed[0];
					} else {
						$row[$columnName] = $processed;
					}
				}
			}
			$rows[] = $row;
		}
		return $rows;
	}
}

class SchemaException extends Exception {
	function __construct($msg)
	{
		file_put_contents( MW_INSTALL_PATH . '/cook.txt', "SCHEMA EXCEPTION " . print_r($msg, true) . "\n" , FILE_APPEND);
		parent::__construct($msg);
	}
}

class QueryException extends Exception {
	function __construct($msg)
	{
		file_put_contents( MW_INSTALL_PATH . '/cook.txt', "QUERY EXCEPTION " . print_r($msg, true) . "\n" , FILE_APPEND);
		parent::__construct($msg);
	}
}
