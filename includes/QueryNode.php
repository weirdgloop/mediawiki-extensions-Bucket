<?php

namespace MediaWiki\Extension\Bucket;

use Wikimedia\Rdbms\IDatabase;
use Wikimedia\Rdbms\IExpression;

/**
 * @property QueryNode[] $children
 */
abstract class QueryNode {
	abstract public function getWhereSQL( IDatabase $dbw ): IExpression;
}
