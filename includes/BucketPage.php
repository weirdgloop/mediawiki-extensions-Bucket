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
		$out->disableClientCache(); // DEBUG
		$title = $this->getTitle();
		$out->setPageTitle( $title );

		$dbw = MediaWikiServices::getInstance()->getDBLoadBalancer()->getConnectionRef( DB_PRIMARY );
		$linkRenderer = MediaWikiServices::getInstance()->getLinkRenderer();

		$table_name = Bucket::getValidFieldName( $title->getRootText() );

		$res = $dbw->newSelectQueryBuilder()
					->from( 'bucket_schemas' )
					->select( [ 'table_name', 'backing_table_name', 'schema_json' ] )
					->where( $dbw->makeList( [ 'table_name' => $table_name, 'backing_table_name' => $table_name ], LIST_OR ) )
					->caller( __METHOD__ )
					->fetchResultSet();
		$schemas = [];
		$backingBucketName = [];
		foreach ( $res as $row ) {
			$schemas[$row->table_name] = json_decode( $row->schema_json, true );
			$backingBucketName[$row->table_name] = $row->backing_table_name;
			// Buckets pointing to this bucket
			if ( $row->table_name != $table_name ) {
				$link = $linkRenderer->makeKnownLink( new TitleValue( NS_BUCKET, $row->table_name ) );
				$out->addHTML( '<h3>' . wfMessage( 'bucket-page-redirect-here-warning', $link )->text() . '</h3>' );
			}
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

		if ( $backingBucketName[$table_name] !== null ) {
			$link = $linkRenderer->makeKnownLink( new TitleValue( NS_BUCKET, $backingBucketName[$table_name] ) );
			$out->addHTML( '<h3>' . wfMessage( 'bucket-page-redirects-to-warning', $link )->text() . '</h3>' );
		}

		$resultCount = count( $queryResult );
		$endResult = $offset + $resultCount;
		// TODO: I really want to show the total row count for the table
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
