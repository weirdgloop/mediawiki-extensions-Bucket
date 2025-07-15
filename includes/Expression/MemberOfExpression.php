<?php

namespace MediaWiki\Extension\Bucket\Expression;

use InvalidArgumentException;
use Wikimedia\Rdbms\Database\DbQuoter;
use Wikimedia\Rdbms\Expression;

class MemberOfExpression extends Expression {
	private string $field;
	private string $op;
	private $value;

	public function __construct( string $field, string $op, $value ) {
		if ( is_array( $value ) || $value === null ) {
			throw new InvalidArgumentException( "MEMBER OF can't take array or null as value" );
		}
		if ( !in_array( $op, [ '!=', '=' ] ) ) {
			throw new InvalidArgumentException( "Operator $op can't be used with MEMBER OF" );
		}
		parent::__construct( $field, $op, $value );
		$this->field = $field;
		$this->op = $op;
		$this->value = $value;
	}

	public function toSql( DbQuoter $dbQuoter ): string {
		if ( $this->op === '!=' ) {
			return ( new NotExpression( $this ) )->toSql( $dbQuoter );
		}
		return $dbQuoter->addQuotes( $this->value ) . ' MEMBER OF(' . $this->field . ')';
	}

	public function toGeneralizedSql(): string {
		if ( $this->op === '!=' ) {
			return ( new NotExpression( $this ) )->toGeneralizedSql();
		}
		return '?' . ' MEMBER OF(' . $this->field . ')';
	}
}
