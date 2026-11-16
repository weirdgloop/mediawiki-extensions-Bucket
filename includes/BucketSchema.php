<?php

namespace MediaWiki\Extension\Bucket;

use JsonSerializable;
use Wikimedia\Rdbms\IDatabase;

/**
 * @property BucketSchemaField[] $fields
 */
class BucketSchema implements JsonSerializable {
	private string $bucketName;
	private array $fields = [];
	private int $timestamp;

	public function __construct( string $bucketName, array $schema ) {
		$this->timestamp = time();
		if ( $bucketName === '' ) {
			throw new QueryException( wfMessage( 'bucket-empty-bucket-name' ) );
		}
		$this->bucketName = $bucketName;

		foreach ( $schema as $name => $val ) {
			if ( $val instanceof BucketSchemaField ) {
				$this->fields[$val->getFieldName()] = $val;
			} else {
				// Skip the _time field, its not a real field
				if ( $name === '_time' ) {
					$this->timestamp = $val;
					continue;
				}
				$this->fields[$name] = new BucketSchemaField(
					$name,
					BucketValueType::from( $val['type'] ),
					$val['index'],
					$val['repeated']
				);
			}
		}
	}

	/**
	 * @return BucketSchemaField[]
	 */
	public function getFields(): array {
		return $this->fields;
	}

	public function getField( string $fieldName ): BucketSchemaField {
		return $this->fields[$fieldName];
	}

	public function getName(): string {
		return $this->bucketName;
	}

	public function getTableName(): string {
		return BucketDatabase::getBucketTableName( $this->bucketName );
	}

	public function getSafe( IDatabase $dbw ): string {
		return $dbw->addIdentifierQuotes( $this->getTableName() );
	}

	public function jsonSerialize(): array {
		$fields = $this->getFields();
		$fields['_time'] = $this->timestamp;
		return $fields;
	}
}
