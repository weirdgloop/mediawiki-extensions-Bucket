<?php

namespace MediaWiki\Extension\Bucket;

use MediaWiki\Html\TemplateParser;
use MediaWiki\SpecialPage\SpecialPage;
use OOUI;

class SpecialBucket extends SpecialPage {
	private TemplateParser $templateParser;

	private BucketDatabase $bucketDb;

	private BucketPageHelper $bucketPageHelper;

	public function __construct( BucketDatabase $bucketDb, BucketPageHelper $bucketPageHelper ) {
		parent::__construct( 'Bucket' );
		$this->templateParser = new TemplateParser( __DIR__ . '/Templates' );
		$this->bucketDb = $bucketDb;
		$this->bucketPageHelper = $bucketPageHelper;
	}

	/**
	 * @param string $bucket
	 * @param string $select
	 * @param string $where
	 * @param int $limit
	 * @param int $offset
	 * @return string
	 * @throws OOUI\Exception
	 */
	private function getQueryBuilder( $bucket, $select, $where, $limit, $offset ) {
		$inputs = [];
		$inputs[] = new OOUI\FieldLayout(
			new OOUI\TextInputWidget(
				[
					'name' => 'bucket',
					'value' => $bucket
				]
			),
			[
				'align' => 'right',
				'label' => 'Bucket',
				'help' => $this->msg( 'bucket-view-help-bucket-name' )
			]
		);
		$inputs[] = new OOUI\FieldLayout(
			new OOUI\TextInputWidget(
				[
					'name' => 'select',
					'value' => $select,
				]
			),
			[
				'align' => 'right',
				'label' => 'Select',
				'help' => $this->msg( 'bucket-view-help-select' )
			]
		);
		$inputs[] = new OOUI\FieldLayout(
			new OOUI\TextInputWidget(
				[
					'name' => 'where',
					'value' => $where,
				]
			),
			[
				'align' => 'right',
				'label' => 'Where',
				'help' => $this->msg( 'bucket-view-help-where' )
			]
		);
		$inputs[] = new OOUI\FieldLayout(
			new OOUI\NumberInputWidget(
				[
					'name' => 'limit',
					'value' => $limit,
					'min' => 1,
					'max' => 500
				]
			),
			[
				'align' => 'right',
				'label' => 'Limit',
				'help' => $this->msg( 'bucket-view-help-limit' )
			]
		);
		$inputs[] = new OOUI\FieldLayout(
			new OOUI\NumberInputWidget(
				[
					'name' => 'offset',
					'value' => $offset,
					'min' => 0
				]
			),
			[
				'align' => 'right',
				'label' => 'Offset',
				'help' => $this->msg( 'bucket-view-help-offset' )
			]
		);
		$inputs[] = new OOUI\FieldLayout(
			new OOUI\ButtonInputWidget(
				[
					'type' => 'submit',
					'label' => $this->msg( 'bucket-view-submit' ),
					'align' => 'center'

				] ),
				[
					'label' => ' '
				]
		);

		$form = new OOUI\FormLayout( [
			'items' => $inputs,
			'action' => $this->getPageTitle()->getLocalURL(),
			'method' => 'get'
		] );

		return $form . '<br>';
	}

	/**
	 * @param string|null $subPage
	 * @return void
	 * @throws OOUI\Exception
	 */
	public function execute( $subPage ) {
		$request = $this->getRequest();
		$out = $this->getOutput();
		$this->setHeaders();
		$out->enableOOUI();
		$out->addModuleStyles( 'ext.bucket.bucketpage.styles' );
		$out->setPageTitle( 'Bucket browse' );

		$bucket = $request->getText( 'bucket', '' );
		$select = $request->getText( 'select', '*' );
		$where = $request->getText( 'where', '' );
		$limit = $request->getInt( 'limit', 20 );
		$offset = $request->getInt( 'offset', 0 );

		$out->addHTML( $this->getQueryBuilder( $bucket, $select, $where, $limit, $offset ) );

		if ( $bucket === '' ) {
			return;
		}

		try {
			$bucketName = Bucket::getValidFieldName( $bucket );
		} catch ( SchemaException ) {
			$out->addWikiTextAsContent( $this->bucketPageHelper->printError(
				$this->msg( 'bucket-query-bucket-invalid', $bucket )->parse() ) );
			return;
		}

		$dbw = $this->bucketDb->getDB();
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

		$fullResult = $this->bucketPageHelper->runQuery( $request, $bucket, $select, $where, $limit, $offset );
		$queryResult = [];

		if ( isset( $fullResult['error'] ) ) {
			$out->addWikiTextAsContent( $this->bucketPageHelper->printError( $fullResult['error'] ) );
			return;
		} elseif ( isset( $fullResult['bucket'] ) ) {
			$queryResult = $fullResult['bucket'];
		}

		$resultCount = count( $fullResult['bucket'] );
		$endResult = $offset + $resultCount;

		$html = $this->templateParser->processTemplate(
			'BucketPageView',
			[
				'resultHeaderText' => $out->msg( 'bucket-page-result-counter' )
					->numParams( $resultCount, $offset, $endResult )->parse(),
				'paginationLinks' => $this->bucketPageHelper->getPageLinks(
					$this->getFullTitle(), $limit, $offset, $request->getQueryValues(), ( $resultCount === $limit ) ),
				'resultTable' => $this->bucketPageHelper->getResultTable(
					$this->templateParser, $schemas[$bucketName], $fullResult['fields'], $queryResult )
			]
		);
		$out->addHTML( $html );
	}

	/**
	 * @return string
	 */
	protected function getGroupName() {
		return 'bucket';
	}
}
