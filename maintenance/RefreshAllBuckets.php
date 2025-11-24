<?php

namespace MediaWiki\Extension\Bucket;

use MediaWiki\Deferred\LinksUpdate\LinksUpdate;
use MediaWiki\Maintenance\Maintenance;
use MediaWiki\MediaWikiServices;

$IP = getenv( 'MW_INSTALL_PATH' );
if ( $IP === false ) {
	$IP = __DIR__ . '/../../..';
}
require_once "$IP/maintenance/Maintenance.php";

/**
 * Queue refresh links for all bucket writers
 */
class RefreshAllBuckets extends Maintenance {
	public function __construct() {
		parent::__construct();
		$this->requireExtension( 'Bucket' );
		$this->addDescription( 'Queue refresh links for all bucket writers' );
	}

	public function execute() {
		$services = MediaWikiServices::getInstance();
		$bucketNS = $services->getNamespaceInfo()->getCanonicalIndex( 'bucket' );
		$pages = $services->getPageStore()->newSelectQueryBuilder()
			->where( [ 'page_namespace' => $bucketNS ] )
			->fetchPageRecords();
		foreach ( $pages as $page ) {
			$this->output( "Queuing page: {$page->getDBkey()}\n" );
			LinksUpdate::queueRecursiveJobsForTable(
				$page,
				'templatelinks',
				'bucket-reparse',
				'unknown',
				MediaWikiServices::getInstance()->getBacklinkCacheFactory()->getBacklinkCache( $page )
			);
		}
		return true;
	}
}

$maintClass = RefreshAllBuckets::class;
require_once RUN_MAINTENANCE_IF_MAIN;
