<?php

namespace MediaWiki\Extension\Bucket;

use MediaWiki\Extension\Bucket\Expression\NotExpression;
use Wikimedia\Rdbms\IDatabase;
use Wikimedia\Rdbms\IExpression;

class NotNode extends QueryNode {
	private QueryNode $child;

	public function __construct( QueryNode $child ) {
		$this->child = $child;
	}

	public function getWhereSQL( IDatabase $dbw ): IExpression {
		return new NotExpression( $this->child->getWhereSQL( $dbw ) );
	}
}
