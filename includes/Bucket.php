<?php

namespace MediaWiki\Extension\Bucket;

use MediaWiki\Logger\LoggerFactory;
use MediaWiki\Title\Title;
use Wikimedia\Rdbms\IExpression;

class Bucket {
	public const EXTENSION_DATA_KEY = 'bucket:puts';
	public const EXTENSION_BUCKET_NAMES_KEY = 'bucket:puts_bucket_names';
	public const ISSUES_BUCKET = 'bucket_issues';
	public const TEXT_BYTE_LIMIT = 65535;

	private static function isValidName( string $name ): bool {
		if (
			// Disallow numeric names as the MW RDBMS treats numeric tables names as ints in some circumstances.
			!is_numeric( $name )
			&& !str_starts_with( $name, '_' )
			&& !str_contains( $name, '__' )
			&& preg_match( '/^[a-zA-Z0-9_]+$/D', $name )
		) {
			return true;
		}
		return false;
	}

	public static function getValidFieldName( string $bucketName, string $fieldName ): string {
		if ( self::isValidName( $fieldName ) ) {
			$cleanName = strtolower( trim( $fieldName ) );
			$dbw = BucketDatabase::getDB();
			$fullName = $dbw->tableName( BucketDatabase::getSubTableName( $bucketName, $cleanName ), 'raw' );
			// MySQL has a maximum of 64 characters for a table name
			if ( strlen( $fullName ) <= 64 ) {
				return $cleanName;
			} else {
				throw new SchemaException(
					wfMessage( 'bucket-long-field-name-error' )
						->params( $fieldName )
						->numParams( [
							strlen( $cleanName ),
							64 - strlen( $dbw->tableName( BucketDatabase::getSubTableName( $bucketName, '' ), 'raw' ) )
						] ) );
			}
		}
		throw new SchemaException( wfMessage( 'bucket-schema-invalid-field-name', $fieldName ) );
	}

	public static function getValidBucketName( string $bucketName ): string {
		if ( ucfirst( $bucketName ) !== ucfirst( strtolower( $bucketName ) ) ) {
			throw new SchemaException( wfMessage( 'bucket-capital-name-error' ) );
		}
		if ( self::isValidName( $bucketName ) ) {
			$cleanName = strtolower( trim( $bucketName ) );
			$dbw = BucketDatabase::getDB();
			$fullName = $dbw->tableName( BucketDatabase::getSubTableName( $bucketName, '' ), 'raw' );
			if ( strlen( $fullName ) <= 55 ) {
				return $cleanName;
			} else {
				throw new SchemaException(
					wfMessage( 'bucket-long-bucket-name-error' )
						->params( $bucketName )
						->numParams( [
							strlen( $cleanName ),
							55 - strlen( $dbw->tableName( BucketDatabase::getBucketTableName( '' ), 'raw' ) )
						] ) );
			}
		}
		throw new SchemaException( wfMessage( 'bucket-invalid-name-warning', $bucketName ) );
	}

	public static function writePuts( int $pageId, string $titleText, array $puts ) {
		( new BucketWriter() )->writePuts( $pageId, $titleText, $puts );
	}

	/**
	 * @param array $userInput
	 * @return array
	 */
	public static function runSelect( array $userInput, ?Title $title ): array {
		$query = new BucketQuery( $userInput );
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
				if ( !is_numeric( $key ) ) {
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

		$generalized_sql = null;
		$conds = $selectQueryBuilder->getQueryInfo()['conds'];
		if ( is_array( $conds ) && count( $conds ) > 0 && $conds[0] instanceof IExpression ) {
			$generalized_sql = $conds[0]->toGeneralizedSql();
		}

		LoggerFactory::getInstance( 'bucket' )->debug( 'bucket query',
			[
				'bucket_count' => count( $result ),
				'bucket_sql' => $selectQueryBuilder->getSQL(),
				'bucket_generalized_sql' => $generalized_sql,
				'bucket_page' => $title ? $title->__toString() : null,
			] );

		return [ $result, $sql_string ];
	}
}
