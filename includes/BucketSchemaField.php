<?php

namespace MediaWiki\Extension\Bucket;

use JsonSerializable;
use MediaWiki\Extension\Bucket\Exception\BucketException;

class BucketSchemaField implements JsonSerializable {
	private string $fieldName;
	private BucketValueType $type;
	private bool $indexed;
	private bool $repeated;

	public function __construct( string $fieldName, BucketValueType $type, bool $indexed, bool $repeated ) {
		$this->fieldName = $fieldName;
		$this->type = $type;
		$this->indexed = $indexed;
		$this->repeated = $repeated;
	}

	public function getFieldName(): string {
		return $this->fieldName;
	}

	public function getType(): BucketValueType {
		return $this->type;
	}

	public function getIndexed(): bool {
		return $this->indexed;
	}

	public function getRepeated(): bool {
		return $this->repeated;
	}

	public function jsonSerialize(): array {
		return [
			'type' => $this->getType()->value,
			'index' => $this->getIndexed(),
			'repeated' => $this->getRepeated()
		];
	}

	/**
	 * Example json: {"type":"INTEGER","index":false,"repeated":false}
	 */
	public static function fromJson( string $fieldName, string $json ): BucketSchemaField {
		$jsonRow = json_decode( $json, true );
		// This is the old style (Pre-July 2025) of table comment formatting.
		// Example json: {"_page_id":{"type":"INTEGER","index":false,"repeated":false}}
		// This can be removed once old buckets are no longer used (after Bucket is in prod)
		if ( isset( $jsonRow[$fieldName]['type'] ) ) {
			$jsonRow = $jsonRow[$fieldName];
		}
		// End old style handling
		return new BucketSchemaField(
			$fieldName, BucketValueType::from( $jsonRow['type'] ), $jsonRow['index'], $jsonRow['repeated'] );
	}

	public function getDatabaseValueType(): DatabaseValueType {
		if ( $this->getRepeated() ) {
			return DatabaseValueType::Json;
		}
		return $this->getSubDatabaseValueType();
	}

	public function getSubDatabaseValueType(): DatabaseValueType {
		switch ( $this->getType() ) {
			case BucketValueType::Text:
			case BucketValueType::Page:
				return DatabaseValueType::Text;
			case BucketValueType::Boolean:
				return DatabaseValueType::Boolean;
			case BucketValueType::Double:
				return DatabaseValueType::Double;
			case BucketValueType::Integer:
				return DatabaseValueType::Integer;
			default:
				return DatabaseValueType::Text;
		}
	}

	public function castValueForDatabase( mixed $value ): mixed {
		if ( $this->getRepeated() ) {
			if ( !is_array( $value ) ) {
				// Wrap single values in an array for compatability
				$value = [ $value ];
			}
			$value = array_values( $value );
			$outputValues = [];
			foreach ( $value as $single ) {
				if ( $single === null ) {
					continue;
				}
				$outputValues[] = $this->castSubValueForDatabase( $single );
			}
			if ( count( $outputValues ) === 0 ) {
				return null;
			}
			$jsonValue = json_encode( $outputValues );
			if ( strlen( $jsonValue ) > Bucket::TEXT_BYTE_LIMIT ) {
				throw new BucketException( wfMessage( 'bucket-put-repeated-total-too-long' )
					->numParams( strlen( $jsonValue ), Bucket::TEXT_BYTE_LIMIT ) );
			}
			return $jsonValue;
		}
		return $this->castSubValueForDatabase( $value );
	}

	public function castSubValueForDatabase( mixed $value ): mixed {
		if ( $value === null ) {
			return null;
		}
		switch ( $this->getSubDatabaseValueType() ) {
			case DatabaseValueType::Text:
				if ( is_array( $value ) ) {
					$value = json_encode( $value );
				} else {
					$value = strval( $value );
				}
				if ( strlen( $value ) > Bucket::TEXT_BYTE_LIMIT ) {
					throw new BucketException( wfMessage( 'bucket-put-text-too-long' )
						->numParams( strlen( $value ), Bucket::TEXT_BYTE_LIMIT ) );
				}
				return $value;
			case DatabaseValueType::Double:
				return floatval( $value );
			case DatabaseValueType::Integer:
				return intval( $value );
			case DatabaseValueType::Boolean:
				$bool_value = filter_var( $value, FILTER_VALIDATE_BOOL, FILTER_NULL_ON_FAILURE );
				if ( $bool_value === null ) {
					return null;
				}
				// MySQL uses 1 for true, 0 for false
				return (int)$bool_value;
			default:
				return null;
		}
	}

	/**
	 * @param mixed $value
	 * @return array|bool|float|int|void
	 */
	public function castValueForLua( $value ) {
		if ( $value === null ) {
			return null;
		}
		$type = $this->getType();
		if ( $this->getRepeated() ) {
			$ret = [];
			$jsonData = json_decode( $value, true );
			// If we are in a repeated field but only holding a scalar, make it an array anyway.
			if ( !is_array( $jsonData ) ) {
				$jsonData = [ $jsonData ];
			}
			$nonRepeatedData = new BucketSchemaField(
				$this->getFieldName(), $this->getType(), $this->getIndexed(), false );
			foreach ( $jsonData as $subVal ) {
				$ret[] = $nonRepeatedData->castValueForLua( $subVal );
			}
			return $ret;
		}
		switch ( $type ) {
			case BucketValueType::Text:
			case BucketValueType::Page:
				return $value;
			case BucketValueType::Double:
				return floatval( $value );
			case BucketValueType::Integer:
				return intval( $value );
			case BucketValueType::Boolean:
				return boolval( $value );
		}
	}
}
