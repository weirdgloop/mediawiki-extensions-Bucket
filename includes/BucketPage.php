<?php

namespace MediaWiki\Extension\Bucket;

use Article;
use MediaWiki\MediaWikiServices;
use Mediawiki\Title\Title;
use MediaWiki\Title\TitleValue;
use Wikimedia\Rdbms\DBQueryTimeoutError;

class BucketPage extends Article {

	public function __construct( Title $title ) {
		parent::__construct( $title );
	}

	public function view() {
		$context = $this->getContext();
		$out = $context->getOutput();
		$request = $context->getRequest();

		parent::view();

		// On diff and oldid pages, show what people would normally expect to see.
		if ( $request->getCheck( 'diff' ) || $this->getOldID() ) {
			return;
		}

		$out->enableOOUI();
		$out->addModuleStyles( 'ext.bucket.bucketpage.styles' );
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

		$select = $request->getText( 'select', '*' );
		$where = $request->getText( 'where', '' );
		$limit = $request->getInt( 'limit', 20 );
		$offset = $request->getInt( 'offset', 0 );

		$fullResult = BucketPageHelper::runQuery( $request, $bucketName, $select, $where, $limit, $offset );

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

		try {
			$maxCount = $dbw->newSelectQueryBuilder()
			->select( 'COUNT(*)' )
			->from( BucketDatabase::getBucketTableName( $bucketName ) )
			->fetchField();

		} catch ( DBQueryTimeoutError ) {
			$maxCount = 'Error';
		}
		$out->addWikiTextAsContent( 'Bucket entries: ' . $maxCount );

		$out->addWikiMsg( 'bucket-page-result-counter', $resultCount, $offset, $endResult );

		$specialQueryValues = $request->getQueryValues();
		unset( $specialQueryValues['action'] );
		unset( $specialQueryValues['title'] );
		$specialQueryValues['bucket'] = $bucketName;
		$out->addHTML( $linkRenderer->makeKnownLink(
			new TitleValue( NS_SPECIAL, 'Bucket' ), wfMessage(
				'bucket-page-dive-into' ), [], $specialQueryValues ) );
		$out->addHTML( '<br>' );

		$pageLinks = BucketPageHelper::getPageLinks(
			$title, $limit, $offset, $request->getQueryValues(), ( $resultCount === $limit ) );

		$out->addHTML( $pageLinks );
		$out->addWikiTextAsContent(
			BucketPageHelper::getResultTable( $schemas[$bucketName], $fullResult['fields'], $queryResult ) );
		$out->addHTML( $pageLinks );
	}
}
