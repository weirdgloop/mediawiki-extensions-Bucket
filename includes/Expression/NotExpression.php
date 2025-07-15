<?php

namespace MediaWiki\Extension\Bucket\Expression;

use Wikimedia\Rdbms\Database\DbQuoter;
use Wikimedia\Rdbms\IExpression;

class NotExpression implements IExpression {
	private IExpression $expr;

	public function __construct( IExpression $expr ) {
		$this->expr = $expr;
	}

	public function toSql( DbQuoter $dbQuoter ): string {
		return 'NOT (' . $this->expr->toSql( $dbQuoter ) . ')';
	}

	public function toGeneralizedSql(): string {
		return 'NOT (' . $this->expr->toGeneralizedSql() . ')';
	}
}
