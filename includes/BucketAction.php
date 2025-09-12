<?php

namespace MediaWiki\Extension\Bucket;

use Action;
use Article;
use MediaWiki\Context\IContextSource;
use MediaWiki\Html\Html;
use MediaWiki\Html\TemplateParser;
use MediaWiki\MediaWikiServices;
use MediaWiki\Title\TitleValue;

class BucketAction extends Action {
	private TemplateParser $templateParser;

	public function __construct( Article $article, IContextSource $context ) {
		parent::__construct( $article, $context );
		$this->templateParser = new TemplateParser( __DIR__ . '/Templates' );
	}

	/**
	 * @return string
	 */
	public function getName() {
		return 'bucket';
	}

	public function show() {
		$out = $this->getOutput();
		$title = $this->getArticle()->getTitle();
		$pageId = $this->getArticle()->getPage()->getId();
		$out->addHelpLink( 'https://meta.weirdgloop.org/Extension:Bucket/Bucket action', true );
		$out->setPageTitleMsg( wfMessage( 'bucket-action-title', $title->getFullText() ) );
		$out->addModuleStyles( [
			'mediawiki.codex.messagebox.styles',
			'ext.bucket.bucketpage.styles'
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

		// Escape title for lua
		$luaTitle = addslashes( $title );
		$linkRenderer = MediaWikiServices::getInstance()->getLinkRenderer();
		foreach ( $buckets as $bucketName ) {
			$bucketTitle = new TitleValue( NS_BUCKET, $bucketName );
			$fullResult = BucketPageHelper::runQuery(
				$this->getRequest(), $bucketTitle->getDBkey(), '*', "{'page_name', '$luaTitle'}", 500, 0 );
			$html = $this->templateParser->processTemplate(
				'BucketAction',
				[
					'headerText' => $linkRenderer->makePreloadedLink( $bucketTitle ),
					'resultTable' => BucketPageHelper::getResultTable( $this->templateParser,
						$schemas[$bucketName], $fullResult['fields'], $fullResult['bucket'] )
				]
			);
			$out->addHTML( $html );
		}
	}

}
