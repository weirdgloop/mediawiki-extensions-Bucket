<?php

namespace MediaWiki\Extension\Bucket;

use MediaWiki\SpecialPage\SpecialPage;

class SpecialAllBuckets extends SpecialPage {
	public function __construct() {
		parent::__construct( 'AllBuckets' );
	}

	/**
	 * @param string|null $subPage
	 * @return void
	 */
	public function execute( $subPage ) {
		$out = $this->getOutput();
		$this->setHeaders();

		$out->setPageTitleMsg( $this->msg( 'allbuckets' ) );

		$dbw = BucketDatabase::getDB();
		$res = $dbw->newSelectQueryBuilder()
			->from( 'bucket_schemas' )
			->select( [ 'bucket_name', 'schema_json' ] )
			->caller( __METHOD__ )
			->fetchResultSet();

		$list = [];
		$list[] = '{|class="wikitable"';
		$list[] = '!' . $this->msg( 'allbuckets-heading' )->parse();
		foreach ( $res as $row ) {
			$list[] = "|-\n|[[Bucket:$row->bucket_name]]";
		}
		$list[] = '|} ';
		$out->addWikiTextAsContent( implode( "\n", $list ) );
	}

	/**
	 * @return string
	 */
	protected function getGroupName() {
		return 'bucket';
	}
}
