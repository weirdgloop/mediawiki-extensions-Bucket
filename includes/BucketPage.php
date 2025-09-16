<?php

namespace MediaWiki\Extension\Bucket;

use Article;
use MediaWiki\Html\TemplateParser;
use MediaWiki\MediaWikiServices;
use MediaWiki\SpecialPage\SpecialPage;
use Mediawiki\Title\Title;
use Wikimedia\Rdbms\DBQueryTimeoutError;

class BucketPage extends Article {
	private TemplateParser $templateParser;

	public function __construct( Title $title ) {
		parent::__construct( $title );
		$this->templateParser = new TemplateParser( __DIR__ . '/Templates' );
	}

	public function view() {
		$context = $this->getContext();
		$out = $context->getOutput();
		$out->addHelpLink( 'https://meta.weirdgloop.org/Extension:Bucket/Bucket namespace', true );
		$request = $context->getRequest();

		parent::view();

		// If the page doesn't exist, then there's no reason to show the bucket
		if ( !$this->getPage()->hasViewableContent() ) {
			return;
		}

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
			$maxCount = '?';
		}

		$html = $this->templateParser->processTemplate(
			'BucketPageView',
			[
				'browseText' => $out->msg( 'bucket-page-browse-text' )->numParams( $maxCount )->parse(),
				'resultHeaderText' => $out->msg( 'bucket-page-result-counter' )
					->numParams(
						$resultCount,
						( $offset == 0 && $endResult == 0 ) ? $offset : $offset + 1,
						$endResult
					)->parse(),
				'paginationLinks' => BucketPageHelper::getPageLinks(
					$title, $limit, $offset, $request->getQueryValues(), ( $resultCount === $limit ) ),
				'resultTable' => BucketPageHelper::getResultTable(
					$this->templateParser, $schemas[$bucketName], $fullResult['fields'], $queryResult ),
				'diveText' => $linkRenderer->makePreloadedLink(
					SpecialPage::getTitleValueFor( 'Bucket' ),
					$out->msg( 'bucket-page-dive-text' )->parse(), '', [], [ 'bucket' => $bucketName ]
				)
			]
		);

		$out->addHTML( $html );
	}
}
