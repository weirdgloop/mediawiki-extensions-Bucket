<?php

namespace MediaWiki\Extension\Bucket;

use Wikimedia\Rdbms\IDatabase;

class BucketQuery {
	public const MAX_LIMIT = 5000;
	public const DEFAULT_LIMIT = 500;
	private const WHERE_OPS = [
		'='  => true,
		'!=' => true,
		'>=' => true,
		'<=' => true,
		'>'  => true,
		'<'  => true,
	];

	private static $allSchemas = [];

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
			$tableName = 'Category';
			$columnName = explode( ':', $column )[1];
			return [
				'fullName' => $dbw->addIdentifierQuotes( $tableName . ':' . $columnName ),
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
		$columnName = Bucket::getValidFieldName( $columnNameTemp );
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
			$columnTableName = Bucket::getValidFieldName( $parts[0] );
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
		$bucketName = Bucket::getBucketTableName( $tableName );
		return [
			'fullName' => $dbw->addIdentifierQuotes( $bucketName ) . '.' . $dbw->addIdentifierQuotes( $columnName ),
			'tableName' => $tableName,
			'columnName' => $columnName,
			'schema' => $schemas[$tableName][$columnName]
		];
	}

	private static function sanitizeValue( $value, IDatabase $dbw ) {
		if ( !is_scalar( $value ) ) {
			throw new QueryException( wfMessage( 'bucket-query-non-scalar' ) );
		}
		if ( is_int( $value ) ) {
			return intval( $value );
		}
		if ( is_float( $value ) ) { // Float and double
			return floatval( $value );
		}
		if ( is_bool( $value ) ) {
			// MySQL doesn't have boolean, 0 = FALSE and 1 = TRUE
			if ( $value ) {
				return 1;
			} else {
				return 0;
			}
		}
		if ( is_string( $value ) ) {
			return $dbw->addQuotes( strval( $value ) );
		}
		throw new QueryException( wfMessage( 'bucket-query-cast-fail', $value ) );
	}

	/**
	 *  $condition is an array of members:
	 * 		operands -> Array of $conditions
	 * 		(optional)op -> AND | OR | NOT
	 * 		unnamed -> scalar value or array of scalar values
	 */
	public static function getWhereCondition( $condition, $fieldNamesToTables, $schemas, IDatabase $dbw, &$categoryJoins, $primaryTableName ) {
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
					$children[] = self::getWhereCondition( $operand, $fieldNamesToTables, $schemas, $dbw, $categoryJoins, $primaryTableName );
				}
			}
			if ( $condition['op'] == 'OR' ) {
				return $dbw->makeList( $children, IDatabase::LIST_OR );
			} else {
				return $dbw->makeList( $children, IDatabase::LIST_AND );
			}
		} elseif ( self::isNot( $condition ) ) {
			$child = self::getWhereCondition( $condition['operand'], $fieldNamesToTables, $schemas, $dbw, $categoryJoins, $primaryTableName );
			return "(NOT $child)";
		} elseif ( is_array( $condition ) && isset( $condition[0] ) && is_array( $condition[0] ) ) {
			// .where{{"a", ">", 0}, {"b", "=", "5"}})
			return self::getWhereCondition( [ 'op' => isset( $condition[ 'op' ] ) ? $condition[ 'op' ] : 'AND', 'operands' => $condition ], $fieldNamesToTables, $schemas, $dbw, $categoryJoins, $primaryTableName );
		} elseif ( is_array( $condition ) && !empty( $condition ) && !isset( $condition[0] ) ) {
			// .where({a = 1, b = 2})
			$operands = [];
			foreach ( $condition as $key => $value ) {
				$operands[] = [ $key, '=', $value ];
			}
			return self::getWhereCondition( [ 'op' => 'AND', 'operands' => $operands ], $fieldNamesToTables, $schemas, $dbw, $categoryJoins, $primaryTableName );
		} elseif ( is_array( $condition ) && isset( $condition[0] ) && isset( $condition[1] ) ) {
			if ( count( $condition ) === 2 ) {
				$condition = [ $condition[0], '=', $condition[1] ];
			}
			$whereTableName = null;
			// If we don't have a period then we must be the primary column.
			if ( count( explode( '.', $condition[0] ) ) == 1 ) {
				$whereTableName = $primaryTableName;
			}
			$columnNameData = self::sanitizeColumnName( $condition[0], $fieldNamesToTables, $schemas, $dbw, $whereTableName );
			if ( !isset( self::WHERE_OPS[$condition[1]] ) ) {
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
				$dbType = Bucket::getDbType( $columnNameData['fullName'], $columnData );
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

		$primaryTableName = Bucket::getValidFieldName( $data['tableName'] );
		if ( !$primaryTableName ) {
			throw new QueryException( wfMessage( 'bucket-invalid-name-warning', $data['tableName'] ) );
		}
		$tableNames[ $primaryTableName ] = true;
		$TABLES[Bucket::getBucketTableName( $primaryTableName )] = Bucket::getBucketTableName( $primaryTableName );

		foreach ( $data['joins'] as $join ) {
			$tableName = Bucket::getValidFieldName( $join['tableName'] );
			if ( !$tableName ) {
				throw new QueryException( wfMessage( 'bucket-invalid-name-warning', $join['tableName'] ) );
			}
			if ( isset( $tableNames[$tableName] ) ) {
				throw new QueryException( wfMessage( 'bucket-select-duplicate-join', $tableName ) );
			}
			$tableNames[$tableName] = true;
			$TABLES[Bucket::getBucketTableName( $tableName )] = Bucket::getBucketTableName( $tableName );
			$join['tableName'] = $tableName;
		}

		$tableNamesList = array_keys( $tableNames );
		foreach ( $tableNames as $tableName => $val ) {
			if ( isset( self::$allSchemas[$tableName] ) && self::$allSchemas[$tableName] ) {
				unset( $tableNames[$tableName] );
			}
		}

		$dbw = Bucket::getDB();
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
		if ( empty( $data['selects'] ) ) {
			throw new QueryException( wfMessage( 'bucket-query-select-empty' ) );
		}

		foreach ( $data['selects'] as $selectColumn ) {
			if ( self::isCategory( $selectColumn ) ) {
				$SELECTS[$selectColumn] = "{$dbw->addIdentifierQuotes($selectColumn)}.cl_to IS NOT NULL";
				$categoryName = explode( ':', $selectColumn )[1];
				$categoryJoins[$categoryName] = $selectColumn;
				$ungroupedColumns[$dbw->addIdentifierQuotes( $selectColumn )] = true;
			} else {
				$selectTableName = null;
				// If we don't have a period then we must be the primary column.
				if ( count( explode( '.', $selectColumn ) ) == 1 ) {
					$selectTableName = $primaryTableName;
				}
				$colData = self::sanitizeColumnName( $selectColumn, $fieldNamesToTables, $schemas, $dbw, $selectTableName );

				if ( $colData['tableName'] != $primaryTableName ) {
					$SELECTS[$colData['tableName'] . '.' . $colData['columnName']] = $colData['fullName'];
				} else {
					$SELECTS[$colData['columnName']] = $colData['fullName'];
				}
				$ungroupedColumns[$colData['fullName']] = true;
			}
		}

		if ( !empty( $data['wheres']['operands'] ) ) {
			$WHERES[] = self::getWhereCondition( $data['wheres'], $fieldNamesToTables, $schemas, $dbw, $categoryJoins, $primaryTableName );
		}

		if ( !empty( $categoryJoins ) ) {

			foreach ( $categoryJoins as $categoryName => $alias ) {
				$TABLES[$alias] = 'categorylinks';
				$bucketName = Bucket::getBucketTableName( $primaryTableName );
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
			$leftDefaultTableName = null;
			// If we don't have a period in the column name it must be a primary table column.
			if ( count( explode( '.', $join['cond'][0] ) ) == 1 ) {
				$leftDefaultTableName = $primaryTableName;
			}
			$leftField = self::sanitizeColumnName( $join['cond'][0], $fieldNamesToTables, $schemas, $dbw, $leftDefaultTableName );
			$isLeftRepeated = $leftField['schema']['repeated'];
			$rightDefaultTableName = null;
			// If we don't have a period in the column name it must be a primary table column.
			if ( count( explode( '.', $join['cond'][1] ) ) == 1 ) {
				$rightDefaultTableName = $primaryTableName;
			}
			$rightField = self::sanitizeColumnName( $join['cond'][1], $fieldNamesToTables, $schemas, $dbw, $rightDefaultTableName );

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

				$LEFT_JOINS[Bucket::getBucketTableName( $join['tableName'] )] = [
					"{$rightField['fullName']} MEMBER OF({$leftField['fullName']})"
				];
			} else {
				$LEFT_JOINS[Bucket::getBucketTableName( $join['tableName'] )] = [
					"{$leftField['fullName']} = {$rightField['fullName']}"
				];
			}
		}

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
			->from( Bucket::getBucketTableName( $primaryTableName ) )
			->select( $SELECTS )
			->where( $WHERES )
			->options( $OPTIONS )
			->caller( __METHOD__ )
			->setMaxExecutionTime( 500 );
		foreach ( $LEFT_JOINS as $alias => $conds ) {
			$tmp->leftJoin( $TABLES[$alias], $alias, $conds );
		}
		if ( isset( $data['orderBy'] ) ) {
			$defaultTableName = null;
			// If we don't have a period in the column name it must be a primary table column.
			if ( count( explode( '.', $data['orderBy']['fieldName'] ) ) == 1 ) {
				$defaultTableName = $primaryTableName;
			}
			$orderName = self::sanitizeColumnName( $data['orderBy']['fieldName'], $fieldNamesToTables, $schemas, $dbw, $defaultTableName )['fullName'];
			if ( $orderName == false ) {
				throw new QueryException( wfMessage( 'bucket-query-column-name-invalid', json_encode( $data['orderBy']['fieldName'] ) ) );
			}
			if ( !isset( $data['orderBy']['direction'] ) || ( $data['orderBy']['direction'] != 'ASC' && $data['orderBy']['direction'] != 'DESC' ) ) {
				throw new QueryException( wfMessage( 'bucket-query-order-by-direction', json_encode( $data['orderBy']['direction'] ) ) );
			}
			if ( !isset( $ungroupedColumns[$orderName] ) ) {
				throw new QueryException( wfMessage( 'bucket-query-order-by-must-select', json_encode( $data['orderBy']['fieldName'] ) ) );
			}
			$tmp->orderBy( $orderName, $data['orderBy']['direction'] );
		}
		$sql_string = '';
		if ( $data['debug'] == true ) {
			$sql_string = $tmp->getSQL();
		}
		$res = $tmp->fetchResultSet();
		foreach ( $res as $row ) {
			$row = (array)$row;
			foreach ( $row as $columnName => $value ) {
				$defaultTableName = null;
				// If we don't have a period in the column name it must be a primary table column.
				if ( count( explode( '.', $columnName ) ) == 1 ) {
					$defaultTableName = $primaryTableName;
				}
				$columnData = self::sanitizeColumnName( $columnName, $fieldNamesToTables, $schemas, $dbw, $defaultTableName );
				$row[$columnName] = self::cast( $value, $columnData['schema'] );
			}
			$rows[] = $row;
		}
		return [ $rows, $sql_string ];
	}
}
