<?php

namespace MediaWiki\Extension\Bucket\Query;

use Wikimedia\Rdbms\IDatabase;
use Wikimedia\Rdbms\IExpression;

class OrNode extends GroupNode {
	public function getWhereSQL( IDatabase $dbw ): IExpression {
		$childSQLs = array_map( static function ( QueryNode $child ) use ( $dbw ) {
			return $child->getWhereSQL( $dbw );
		}, $this->children );
		return $dbw->orExpr( $childSQLs );
	}
}
