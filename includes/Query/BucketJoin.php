<?php

namespace MediaWiki\Extension\Bucket\Query;

use MediaWiki\Extension\Bucket\BucketSchema;
use MediaWiki\Extension\Bucket\Exception\QueryException;
use Wikimedia\Rdbms\IDatabase;

class BucketJoin extends Join {
	private BucketSchema $joinedTable;
	private FieldSelector $selector1;
	private FieldSelector $selector2;

	public function __construct( BucketSchema $joinedTable, string $field1, string $field2, BucketQuery $query ) {
		// TODO What does joining between 2 repeated fields look like?
		// TODO Is performance now better for the joining on one repeated field case?
		$this->joinedTable = $joinedTable;
		$selector1 = new FieldSelector( $field1, $query );
		$selector2 = new FieldSelector( $field2, $query );
		// Cannot join on a repeated field
		if ( $selector1->getFieldSchema()->getRepeated() ) {
			throw new QueryException( wfMessage(
				'bucket-query-invalid-join-repeated', $selector1->getFieldSchema()->getFieldName() ) );
		}
		if ( $selector2->getFieldSchema()->getRepeated() ) {
			throw new QueryException( wfMessage(
				'bucket-query-invalid-join-repeated', $selector2->getFieldSchema()->getFieldName() ) );
		}
		// Cannot join with yourself
		if ( $selector1->getBucketSchema() === $selector2->getBucketSchema() ) {
			throw new QueryException( wfMessage( 'bucket-query-invalid-join' ) );
		}
		// One of the joined fields needs to be in the joined table
		if ( $selector1->getBucketSchema() !== $joinedTable && $selector2->getBucketSchema() !== $joinedTable ) {
			throw new QueryException( wfMessage( 'bucket-query-invalid-join' ) );
		}

		$this->selector1 = $selector1;
		$this->selector2 = $selector2;
	}

	public function getJoinTable( IDatabase $dbw ): string {
		return $dbw->tableName( $this->joinedTable->getTableName() );
	}

	public function getAlias(): string {
		return $this->joinedTable->getTableName();
	}

	public function getSQL( IDatabase $dbw ): array {
		$selector1 = $this->selector1;
		$selector1Safe = $selector1->getSafe( $dbw );
		$selector2 = $this->selector2;
		$selector2Safe = $selector2->getSafe( $dbw );
		return [ "$selector1Safe = $selector2Safe" ];
	}
}
