<?php

namespace MediaWiki\Extension\Bucket;

use MediaWiki\SpecialPage\SpecialPage;
use OOUI;

class BucketSpecial extends SpecialPage {
	public function __construct() {
		parent::__construct( 'Bucket' );
	}

	private function getQueryBuilder( $lastQuery, $bucket, $select, $where, $limit, $offset ) {
		$inputs = [];
		$inputs[] = new OOUI\HiddenInputWidget(
			[
				'name' => 'title',
				'value' => 'Special:Bucket'
			]
		);
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
				'help' => wfMessage( 'bucket-view-help-bucket-name' )
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
				'help' => wfMessage( 'bucket-view-help-select' )
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
				'help' => wfMessage( 'bucket-view-help-where' )
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
				'help' => wfMessage( 'bucket-view-help-limit' )
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
				'help' => wfMessage( 'bucket-view-help-offset' )
			]
		);
		$inputs[] = new OOUI\FieldLayout(
			new OOUI\ButtonInputWidget(
				[
					'type' => 'submit',
					'label' => wfMessage( 'bucket-view-submit' ),
					'align' => 'center'

				] ),
				[
					'label' => ' '
				]
		);

		$form = new OOUI\FormLayout( [
			'items' => $inputs,
			'action' => 'Special:Bucket',
			'method' => 'get'
		] );

		return $form . '<br>';
	}

	public function execute( $par ) {
		$request = $this->getRequest();
		$out = $this->getOutput();
		$this->setHeaders();
		$out->enableOOUI();
		$out->setPageTitle( 'Bucket browse' );

		$bucket = $request->getText( 'bucket', '' );
		$select = $request->getText( 'select', '*' );
		$where = $request->getText( 'where', '' );
		$limit = $request->getInt( 'limit', 20 );
		$offset = $request->getInt( 'offset', 0 );

		$out->addHTML( $this->getQueryBuilder( $request, $bucket, $select, $where, $limit, $offset ) );
		try {
			$bucketName = Bucket::getValidFieldName( $bucket );
		} catch ( SchemaException $e ) {
			$out->addWikiTextAsContent( BucketPageHelper::printError( wfMessage( 'bucket-query-bucket-invalid', $bucket )->parse() ) );
			return;
		}

		$dbw = BucketDatabase::getDB();
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

		$fullResult = BucketPageHelper::runQuery( $request, $bucket, $select, $where, $limit, $offset );

		if ( isset( $fullResult['error'] ) ) {
			$out->addWikiTextAsContent( BucketPageHelper::printError( $fullResult['error'] ) );
			return;
		}
		if ( isset( $fullResult['bucket'] ) ) {
			$queryResult = $fullResult['bucket'];
		}

		$out->addHTML( '<h2>'  . $out->msg( 'bucket-view-result' ) .'</h2>' );

		$resultCount = count( $fullResult['bucket'] );
		$endResult = $offset + $resultCount;
		$out->addHTML( wfMessage( 'showingresultsinrange', $resultCount, $offset, $endResult ) . '<br>' );

		$pageLinks = BucketPageHelper::getPageLinks( $out, $this->getFullTitle(), $limit, $offset, $request->getQueryValues(), ( $resultCount == $limit ) );

		$out->addHTML( $pageLinks );
		$out->addWikiTextAsContent( BucketPageHelper::getResultTable( $schemas[$bucketName], $fullResult['fields'], $queryResult ) );
		$out->addHTML( $pageLinks );
	}

	function getGroupName() {
		return 'bucket';
	}
}
