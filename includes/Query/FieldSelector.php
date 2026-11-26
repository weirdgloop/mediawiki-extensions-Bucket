<?php

namespace MediaWiki\Extension\Bucket\Query;

use MediaWiki\Extension\Bucket\BucketDatabase;
use MediaWiki\Extension\Bucket\BucketSchema;
use MediaWiki\Extension\Bucket\BucketSchemaField;
use MediaWiki\Extension\Bucket\Exception\QueryException;
use Wikimedia\Rdbms\IDatabase;

class FieldSelector extends Selector {
	private BucketSchema $schema;
	private BucketSchemaField $schemaField;

	public function __construct( string $fullSelector, BucketQuery $query ) {
		// Split on period
		$parts = explode( '.', $fullSelector );
		if ( $fullSelector === '' || count( $parts ) > 2 ) {
			throw new QueryException( wfMessage( 'bucket-query-field-name-invalid', $fullSelector ) );
		}
		$fieldName = end( $parts );
		// If we don't have a period, we are the primary bucket.
		if ( count( $parts ) === 1 ) {
			$this->schema = $query->getPrimaryBucket();
		} else {
			$usedBuckets = $query->getUsedBuckets();
			if ( !isset( $usedBuckets[$parts[0]] ) ) {
				throw new QueryException( wfMessage( 'bucket-query-bucket-not-found', $parts[0] ) );
			}
			$this->schema = $usedBuckets[$parts[0]];
		}
		$fields = $this->schema->getFields();
		if ( !isset( $fields[$fieldName] ) ) {
			throw new QueryException(
				wfMessage( 'bucket-query-field-not-found-in-bucket', $fieldName, $this->schema->getName() ) );
		}
		$this->schemaField = $fields[$fieldName];
	}

	public function getSafe( IDatabase $dbw ): string {
		return $dbw->addIdentifierQuotes( BucketDatabase::getBucketTableName( $this->schema->getName() ) )
			. '.' . $dbw->addIdentifierQuotes( $this->schemaField->getFieldName() );
	}

	public function getUnsafe(): string {
		if ( $this->subquery ) {
			$repeatedFieldTable = BucketDatabase::getSubTableName(
				$this->getBucketSchema()->getName(),
				$this->getFieldSchema()->getFieldName()
			);
			return $repeatedFieldTable . '.' . $this->getFieldSchema()->getFieldName();
		} else {
			return BucketDatabase::getBucketTableName( $this->schema->getName() )
				. '.' . $this->schemaField->getFieldName();
		}
	}

	public function getSelectSQL( IDatabase $dbw ): string {
		return $this->getSafe( $dbw );
	}

	public function getFieldSchema(): BucketSchemaField {
		return $this->schemaField;
	}

	public function getBucketSchema(): BucketSchema {
		return $this->schema;
	}
}
