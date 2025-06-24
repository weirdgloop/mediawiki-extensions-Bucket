<?php

namespace Mediawiki\Extension\Bucket;

use Article;
use MediaWiki\Extension\Bucket\Bucket;
use MediaWiki\Extension\Bucket\BucketPageHelper;
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
		$out->addModuleStyles( 'ext.bucket.bucketpage.css' );
		$title = $this->getTitle();
		$out->setPageTitle( $title );

		$dbw = Bucket::getDB();
		$linkRenderer = MediaWikiServices::getInstance()->getLinkRenderer();

		$table_name = Bucket::getValidFieldName( str_replace( ' ', '_', $title->getRootText() ) );

		$res = $dbw->newSelectQueryBuilder()
					->from( 'bucket_schemas' )
					->select( [ 'table_name', 'schema_json' ] )
					->where( [ 'table_name' => $table_name ] )
					->caller( __METHOD__ )
					->fetchResultSet();
		$schemas = [];
		foreach ( $res as $row ) {
			$schemas[$row->table_name] = json_decode( $row->schema_json, true );
		}

		$select = $context->getRequest()->getText( 'select', '*' );
		$where = $context->getRequest()->getText( 'where', '' );
		$limit = $context->getRequest()->getInt( 'limit', 20 );
		$offset = $context->getRequest()->getInt( 'offset', 0 );

		$fullResult = BucketPageHelper::runQuery( $this->getContext()->getRequest(), $table_name, $select, $where, $limit, $offset );

		if ( isset( $fullResult['error'] ) ) {
			$out->addHTML( $fullResult['error'] );
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
			->from( Bucket::getBucketTableName( $table_name ) )
			->fetchField();
		$out->addWikiTextAsContent( 'Bucket entries: ' . $maxCount );

		$out->addHTML( wfMessage( 'bucket-page-result-counter', $resultCount, $offset, $endResult ) );

		$specialQueryValues = $context->getRequest()->getQueryValues();
		unset( $specialQueryValues['action'] );
		unset( $specialQueryValues['title'] );
		$specialQueryValues['bucket'] = $table_name;
		$out->addHTML( ' ' );
		$out->addHTML( $linkRenderer->makeKnownLink( new TitleValue( NS_SPECIAL, 'Bucket' ), wfMessage( 'bucket-page-dive-into' ), [], $specialQueryValues ) );
		$out->addHTML( '<br>' );

		$pageLinks = BucketPageHelper::getPageLinks( $title, $limit, $offset, $context->getRequest()->getQueryValues(), ( $resultCount == $limit ) );

		$out->addHTML( $pageLinks );
		$out->addWikiTextAsContent( BucketPageHelper::getResultTable( $schemas[$table_name], $fullResult['columns'], $queryResult ) );
		$out->addHTML( $pageLinks );
	}
}
