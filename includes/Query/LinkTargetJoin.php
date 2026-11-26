<?php

namespace MediaWiki\Extension\Bucket\Query;

use MediaWiki\Extension\Bucket\QueryException;
use MediaWiki\Title\Title;
use Wikimedia\Rdbms\IDatabase;

class LinkTargetJoin extends Join {
	private string $categoryName;
	private BucketQuery $query;

	public function __construct( string $categoryName, BucketQuery $query ) {
		if ( !BucketQuery::isCategory( $categoryName ) ) {
			throw new QueryException( wfMessage( 'bucket-query-expected-category', $categoryName ) );
		}
		$this->categoryName = $categoryName;
		$this->query = $query;
	}

	public function getJoinTable( IDatabase $dbw ): string {
		return $dbw->tableName( 'linktarget' );
	}

	public function getAlias(): string {
		return $this->query->getLinkTargetAlias( $this->categoryName );
	}

	public function getSQL( IDatabase $dbw ): array {
		$categoryNameNoPrefix = Title::newFromText( $this->categoryName, NS_CATEGORY )->getDBkey();
		$safeAlias = $dbw->addIdentifierQuotes( $this->getAlias() );
		return [
			"$safeAlias.lt_namespace" => NS_CATEGORY,
			"$safeAlias.lt_title" => $categoryNameNoPrefix,
		];
	}
}
