<?php

namespace MediaWiki\Extension\Bucket;

use MediaWiki\MediaWikiServices;
use MediaWiki\SpecialPage\SpecialPage;
use MediaWiki\Title\TitleValue;

class AllBucketsSpecial extends SpecialPage {
	public function __construct() {
		parent::__construct( 'AllBuckets' );
	}

	public function execute( $par ) {
		$out = $this->getOutput();
		$this->setHeaders();

		$out->setPageTitle( wfMessage( 'bucket-specialpage-all-buckets-title' ) );

		$dbw = Bucket::getDB();
		$res = $dbw->newSelectQueryBuilder()
			->from( 'bucket_schemas' )
			->select( [ 'bucket_name', 'schema_json' ] )
			->caller( __METHOD__ )
			->fetchResultSet();

		$out->addHTML( '<table class="wikitable">' );
		$out->addHTML( '<tr><th>Bucket</th></tr>' );
		$linkRenderer = MediaWikiServices::getInstance()->getLinkRenderer();
		foreach ( $res as $row ) {
			$out->addHTML( '<tr><td>' . $linkRenderer->makePreloadedLink( new TitleValue( NS_BUCKET, $row->bucket_name ) ) . '</td></tr>' );
		}
		$out->addHTML( '</table>' );
	}

	function getGroupName() {
		return 'bucket';
	}
}
