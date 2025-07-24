<?php

namespace MediaWiki\Extension\Bucket;

use Article;
use MediaWiki\Html\Html;
use MediaWiki\MediaWikiServices;
use Mediawiki\Title\Title;
use MediaWiki\Title\TitleValue;

class BucketPage extends Article {

	public function __construct( Title $title ) {
		parent::__construct( $title );
	}

	public function view() {
		parent::view();
		$context = $this->getContext();
		$out = $this->getContext()->getOutput();
		$out->enableOOUI();
		$out->addModuleStyles( [
			'ext.bucket.page.css',
			'mediawiki.codex.messagebox.styles'
		] );
		$title = $this->getTitle();
		$out->setPageTitle( $title );

		$dbw = BucketDatabase::getDB();
		$linkRenderer = MediaWikiServices::getInstance()->getLinkRenderer();

		try {
			$bucketName = Bucket::getValidFieldName( $this->getTitle()->getDBkey() );
		} catch ( SchemaException $e ) {
			$out->addWikiTextAsContent( BucketPageHelper::printError( $e->getMessage() ) );
			return;
		}

		$res = $dbw->newSelectQueryBuilder()
					->from( 'bucket_schemas' )
					->select( [ 'bucket_name', 'schema_json' ] )
					->where( [ 'bucket_name' => $bucketName ] )
					->caller( __METHOD__ )
					->fetchResultSet();
		$schemas = [];
		foreach ( $res as $row ) {
			$schemas[$row->bucket_name] = json_decode( $row->schema_json, true );
		}

		$select = $context->getRequest()->getText( 'select', '*' );
		$where = $context->getRequest()->getText( 'where', '' );
		$limit = $context->getRequest()->getInt( 'limit', 20 );
		$offset = $context->getRequest()->getInt( 'offset', 0 );

		$fullResult = BucketPageHelper::runQuery( $this->getContext()->getRequest(), $bucketName, $select, $where, $limit, $offset );

		if ( isset( $fullResult['error'] ) ) {
			$out->addWikiTextAsContent( BucketPageHelper::printError( $fullResult['error'] ) );
			return;
		}

		$queryResult = [];
		if ( isset( $fullResult['bucket'] ) ) {
			$queryResult = $fullResult['bucket'];
		}

		$resultCount = count( $queryResult );
		$endResult = $offset + $resultCount;

		$maxCount = $dbw->newSelectQueryBuilder()
			->select( 'COUNT(*)' )
			->from( BucketDatabase::getBucketTableName( $bucketName ) )
			->fetchField();
		$out->addWikiTextAsContent( 'Bucket entries: ' . $maxCount );

		$out->addWikiMsg( 'bucket-page-result-counter', $resultCount, $offset, $endResult );

		$specialQueryValues = $context->getRequest()->getQueryValues();
		unset( $specialQueryValues['action'] );
		unset( $specialQueryValues['title'] );
		$specialQueryValues['bucket'] = $bucketName;
		$out->addHTML( ' ' );
		$out->addHTML( $linkRenderer->makeKnownLink( new TitleValue( NS_SPECIAL, 'Bucket' ), wfMessage( 'bucket-page-dive-into' ), [], $specialQueryValues ) );
		$out->addHTML( '<br>' );

		if ( $maxCount === '0' ) {
			return $out->addHTML( Html::noticeBox( $out->msg( 'bucket-empty' )->parse(), 'bucket-empty' ) );
		}

		$pageLinks = BucketPageHelper::getPageLinks( $title, $limit, $offset, $context->getRequest()->getQueryValues(), ( $resultCount === $limit ) );

		$out->addHTML( $pageLinks );
		$out->addWikiTextAsContent( BucketPageHelper::getResultTable( $schemas[$bucketName], $fullResult['fields'], $queryResult ) );
		$out->addHTML( $pageLinks );
	}
}
