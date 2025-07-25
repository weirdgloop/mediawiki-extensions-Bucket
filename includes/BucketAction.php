<?php

namespace MediaWiki\Extension\Bucket;

use Action;
use MediaWiki\Html\Html;
use MediaWiki\MediaWikiServices;
use MediaWiki\Title\TitleValue;

class BucketAction extends Action {
	/**
	 * @return string
	 */
	public function getName() {
		return 'bucket';
	}

	public function show() {
		// We want to use OOUI for consistent styling
		$this->getOutput()->enableOOUI();

		$out = $this->getOutput();
		$title = $this->getArticle()->getTitle();
		$pageId = $this->getArticle()->getPage()->getId();
		$out->setPageTitleMsg( wfMessage( 'bucket-action-title', $title ) );
		$out->addModuleStyles( [
			'mediawiki.codex.messagebox.styles'
		] );

		$dbw = BucketDatabase::getDB();

		$res = $dbw->newSelectQueryBuilder()
			->from( 'bucket_pages' )
			->select( [ 'bucket_name' ] )
			->where( [ '_page_id' => $pageId ] )
			->groupBy( 'bucket_name' )
			->caller( __METHOD__ )
			->fetchResultSet();
		$buckets = [];
		foreach ( $res as $row ) {
			$buckets[] = $row->bucket_name;
		}

		if ( count( $buckets ) === 0 ) {
			$out->addHTML( Html::noticeBox( $out->msg( 'bucket-action-writes-empty' )->parse(), '' ) );
			return;
		}

		$res = $dbw->newSelectQueryBuilder()
			->from( 'bucket_schemas' )
			->select( [ 'bucket_name', 'schema_json' ] )
			->where( [ 'bucket_name' => $buckets ] )
			->caller( __METHOD__ )
			->fetchResultSet();
		$schemas = [];
		foreach ( $res as $row ) {
			$schemas[$row->bucket_name] = json_decode( $row->schema_json, true );
		}

		$title = $dbw->addQuotes( $title );
		$linkRenderer = MediaWikiServices::getInstance()->getLinkRenderer();
		foreach ( $buckets as $bucketName ) {
			$bucket_page_name = str_replace( '_', ' ', $bucketName );

			$out->addHTML( '<h2>' .
				$linkRenderer->makePreloadedLink( new TitleValue( NS_BUCKET, $bucket_page_name ) ) . '</h2>' );

			$fullResult = BucketPageHelper::runQuery(
				$this->getRequest(), $bucketName, '*', "{'page_name', $title}", 500, 0 );

			$out->addWikiTextAsContent( BucketPageHelper::getResultTable(
				$schemas[$bucketName], $fullResult['fields'], $fullResult['bucket'] ) );
		}
	}

}
