<?php

namespace MediaWiki\Extension\Bucket;

use MediaWiki\SpecialPage\SpecialPage;

class AllBucketsSpecial extends SpecialPage {
	public function __construct() {
		parent::__construct( 'AllBuckets' );
	}

	public function execute( $par ) {
		$out = $this->getOutput();
		$this->setHeaders();

		$out->setPageTitleMsg( wfMessage( 'allbuckets' ) );

		$dbw = BucketDatabase::getDB();
		$res = $dbw->newSelectQueryBuilder()
			->from( 'bucket_schemas' )
			->select( [ 'bucket_name', 'schema_json' ] )
			->caller( __METHOD__ )
			->fetchResultSet();

		$list = [];
		$list[] = '{|class="wikitable"';
		$list[] = '!' . wfMessage( 'allbuckets-heading' )->parse();
		foreach ( $res as $row ) {
			$list[] = "|-\n|[[Bucket:$row->bucket_name]]";
		}
		$list[] = '|} ';
		$out->addWikiTextAsContent( implode( "\n", $list ) );
	}

	function getGroupName() {
		return 'bucket';
	}
}
