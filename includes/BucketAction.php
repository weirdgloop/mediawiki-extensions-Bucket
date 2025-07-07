<?php

namespace MediaWiki\Extension\Bucket;

use Action;
use MediaWiki\MediaWikiServices;
use MediaWiki\Title\TitleValue;

class BucketAction extends Action {

	public function getName() {
		return 'bucket';
	}

	public function show() {
		$this->getOutput()->enableOOUI(); // We want to use OOUI for consistent styling

		$out = $this->getOutput();
		$title = $this->getArticle()->getTitle();
		$pageId = $this->getArticle()->getPage()->getId();
		$out->setPageTitle( "Bucket View: $title" );

		$dbw = Bucket::getDB();

		$res = $dbw->newSelectQueryBuilder()
			->from( 'bucket_pages' )
			->select( [ 'table_name' ] )
			->where( [ '_page_id' => $pageId ] )
			->groupBy( 'table_name' )
			->caller( __METHOD__ )
			->fetchResultSet();
		$buckets = [];
		foreach ( $res as $row ) {
			$buckets[] = $row->table_name;
		}

		if ( count( $buckets ) == 0 ) {
			$out->addWikiTextAsContent( wfMessage( 'bucket-action-writes-empty' ) );
			return;
		}

		$res = $dbw->newSelectQueryBuilder()
			->from( 'bucket_schemas' )
			->select( [ 'table_name', 'schema_json' ] )
			->where( [ 'table_name' => $buckets ] )
			->caller( __METHOD__ )
			->fetchResultSet();
		$schemas = [];
		foreach ( $res as $row ) {
			$schemas[$row->table_name] = json_decode( $row->schema_json, true );
		}

		$title = $dbw->addQuotes( $title );
		$linkRenderer = MediaWikiServices::getInstance()->getLinkRenderer();
		foreach ( $buckets as $bucketName ) {
			$bucket_page_name = str_replace( '_', ' ', $bucketName );

			$out->addHTML( '<h2>' . $linkRenderer->makePreloadedLink( new TitleValue( NS_BUCKET, $bucket_page_name ) ) . '</h2>' );

			$fullResult = BucketPageHelper::runQuery( $this->getRequest(), $bucketName, '*', "{'page_name', $title}", 500, 0 );

			$out->addWikiTextAsContent( BucketPageHelper::getResultTable( $schemas[$bucketName], $fullResult['fields'], $fullResult['bucket'] ) );
		}
	}

}
