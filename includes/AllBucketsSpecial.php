<?php

namespace MediaWiki\Extension\Bucket;

use MediaWiki\MediaWikiServices;
use MediaWiki\SpecialPage\SpecialPage;
use MediaWiki\Title\TitleValue;

class AllBucketsSpecial extends SpecialPage {
	public function __construct() {
		parent::__construct( 'Allbuckets' );
	}

	public function execute( $par ) {
		$out = $this->getOutput();
		$this->setHeaders();

		$out->setPageTitle( wfMessage( 'bucket-specialpage-all-buckets-title' ) );

		$dbw = Bucket::getDB();
		$res = $dbw->newSelectQueryBuilder()
			->from( 'bucket_schemas' )
			->select( [ 'table_name', 'schema_json' ] )
			->caller( __METHOD__ )
			->fetchResultSet();

		$schemas = [];
		foreach ( $res as $row ) {
			$schemas[$row->table_name] = json_decode( $row->schema_json, true );
		}

		$out->addHTML( '<table class="wikitable">' );
		$out->addHTML( '<tr><th>Bucket</th></tr>' );
		$linkRenderer = MediaWikiServices::getInstance()->getLinkRenderer();
		foreach ( $schemas as $row => $val ) {
			$out->addHTML( '<tr><td>' . $linkRenderer->makePreloadedLink( new TitleValue( NS_BUCKET, $row ) ) . '</td></tr>' );
		}
		$out->addHTML( '</table>' );
	}

	function getGroupName() {
		return 'bucket';
	}
}
