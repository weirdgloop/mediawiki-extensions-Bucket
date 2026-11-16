<?php

namespace MediaWiki\Extension\Bucket;

use Wikimedia\Rdbms\IDatabase;
use Wikimedia\Rdbms\IExpression;
use Wikimedia\Rdbms\RawSQLExpression;
use Wikimedia\Rdbms\Subquery;

class SubqueryNode extends QueryNode {
	private FieldSelector $selector;
	private QueryNode $child;

	public function __construct( FieldSelector $selector, QueryNode $child ) {
		$this->selector = $selector;
		$this->child = $child;
	}

	public function getWhereSQL( IDatabase $dbw ): IExpression {
		$selector = $this->selector;
		$repeatedFieldTable = BucketDatabase::getSubTableName(
			$selector->getBucketSchema()->getName(),
			$selector->getFieldSchema()->getFieldName() );
		$subquery = $dbw->newSelectQueryBuilder()
			->from( $repeatedFieldTable )
			->select( [ '_page_id', '_index' ] )
			->where( $this->child->getWhereSQL( $dbw ) )
			->caller( __METHOD__ );
		$subquery = $subquery->getSQL();
		// Should not be $dbw->tableName because the un-prefixed table name is an alias in the subquery
		$tableNameSafe = $dbw->addIdentifierQuotes( $selector->getBucketSchema()->getTableName() );
		$in = "($tableNameSafe._page_id, $tableNameSafe._index) IN ";
		return new RawSQLExpression( $in . new Subquery( $subquery ) );
	}
}
