<?php

namespace MediaWiki\Extension\Bucket;

use MediaWiki\Title\Title;
use Wikimedia\Rdbms\IDatabase;

class CategoryJoin extends Join {
	private CategorySelector $categorySelector;
	private string $categoryName;
	private BucketQuery $query;

	public function __construct( string $categoryName, BucketQuery $query ) {
		if ( !BucketQuery::isCategory( $categoryName ) ) {
			throw new QueryException( wfMessage( 'bucket-query-expected-category', $categoryName ) );
		}
		$this->categorySelector = new CategorySelector( $categoryName, $query );
		$this->categoryName = $categoryName;
		$this->query = $query;
	}

	public function getJoinTable( IDatabase $dbw ): string {
		return $dbw->tableName( 'categorylinks' );
	}

	public function getAlias(): string {
		return $this->query->getCategoryAlias( $this->categoryName );
	}

	public function getSQL( IDatabase $dbw ): array {
		$bucketTableName = $this->query->getPrimaryBucket()->getSafe( $dbw );
		$categoryNameNoPrefix = Title::newFromText( $this->categoryName, NS_CATEGORY )->getDBkey();
		$safeAlias = $dbw->addIdentifierQuotes( $this->getAlias() );
		$safeLinkTargetAlias = $dbw->addIdentifierQuotes( $this->query->getLinkTargetAlias( $this->categoryName ) );
		$conds = [
			// Must be all in one string to avoid the table name being treated as a string value.
			"$safeAlias.cl_from = $bucketTableName._page_id",
			"$safeLinkTargetAlias.lt_id = $safeAlias.cl_target_id",
		];
		if ( !$this->query->useLinkTarget ) {
			$conds["{$this->categorySelector->getSafe($dbw)}"] = $categoryNameNoPrefix;
		}
		return $conds;
	}
}
