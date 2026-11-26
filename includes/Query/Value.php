<?php

namespace MediaWiki\Extension\Bucket\Query;

use MediaWiki\Extension\Bucket\Exception\QueryException;

class Value {
	private mixed $value;

	/**
	 * @param mixed $value
	 */
	public function __construct( $value ) {
		if ( $value !== null && !is_scalar( $value ) ) {
			throw new QueryException( wfMessage( 'bucket-query-non-scalar' ) );
		}
		$this->value = $value;
	}

	/**
	 * @return mixed
	 */
	public function getValue() {
		return $this->value;
	}
}
