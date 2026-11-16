<?php

namespace MediaWiki\Extension\Bucket;

use Wikimedia\Rdbms\IDatabase;

class CategorySelector extends Selector {
	private string $categoryName;
	private BucketQuery $query;

	public function __construct( string $categoryName, BucketQuery $query ) {
		if ( !BucketQuery::isCategory( $categoryName ) ) {
			throw new QueryException( wfMessage( 'bucket-query-expected-category', $categoryName ) );
		}
		$this->categoryName = $categoryName;
		$this->query = $query;
	}

	public function getSafe( IDatabase $dbw ): string {
		if ( $this->query->useLinkTarget ) {
			return $dbw->addIdentifierQuotes( $this->query->getCategoryAlias( $this->categoryName ) ) . '.cl_target_id';
		} else {
			return $dbw->addIdentifierQuotes( $this->query->getCategoryAlias( $this->categoryName ) ) . '.cl_to';
		}
	}

	public function getUnsafe(): string {
		if ( $this->query->useLinkTarget ) {
			return $this->query->getCategoryAlias( $this->categoryName ) . '.cl_target_id';
		} else {
			return $this->query->getCategoryAlias( $this->categoryName ) . '.cl_to';
		}
	}

	public function getSelectSQL( IDatabase $dbw ): string {
		return $dbw->expr( $this->getUnsafe(), '!=', null )->toSql( $dbw );
	}
}
