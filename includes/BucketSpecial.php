<?php

namespace MediaWiki\Extension\Bucket;

use MediaWiki\Html\Html;
use MediaWiki\HTMLForm\HTMLForm;
use MediaWiki\SpecialPage\FormSpecialPage;

class BucketSpecial extends FormSpecialPage {
	private array $formData;

	public function __construct() {
		parent::__construct( 'Bucket' );
	}

	protected function getFormFields() {
		$out = $this->getOutput();
		$out->addModules( [
			'ext.bucket.form'
		] );
		$out->addModuleStyles( [
			'mediawiki.codex.messagebox.styles',
			'mediawiki.special'
		] );

		return [
			'bucket' => [
				'type' => 'title',
				'name' => 'bucket',
				'label-message' => 'bucket-view-bucket-name',
				'help-message' => 'bucket-view-help-bucket-name',
				'help-inline' => false,
				'namespace' => NS_BUCKET,
				'relative' => true,
			],
			'select' => [
				'type' => 'text',
				'name' => 'select',
				'label-message' => 'bucket-view-select',
				'help-message' => 'bucket-view-help-select',
				'help-inline' => false,
				'default' => '*',
			],
			'where' => [
				'type' => 'textarea',
				'name' => 'where',
				'label-message' => 'bucket-view-where',
				'help-message' => 'bucket-view-help-where',
				'help-inline' => false,
				'rows' => 3,
			],
			'limit' => [
				'type' => 'int',
				'name' => 'limit',
				'label-message' => 'bucket-view-limit',
				'help-message' => 'bucket-view-help-limit',
				'help-inline' => false,
				'default' => 20,
				'min' => 1,
				'max' => 500,
			],
			'offset' => [
				'type' => 'int',
				'name' => 'offset',
				'label-message' => 'bucket-view-offset',
				'help-message' => 'bucket-view-help-offset',
				'help-inline' => false,
				'default' => 0,
				'min' => 0,
			],
		];
	}

	protected function alterForm( HTMLForm $form ) {
		$form->setWrapperLegendMsg( 'bucket' );
	}

	public function onSubmit(array $data /* HTMLForm $form = null */) {
		$this->formData = $data;
		return true;
	}

	protected function getShowAlways() {
		return true;
	}

	public function requiresPost() {
		return false;
	}

	protected function getDisplayFormat() {
		return 'ooui';
	}

	public function onSuccess() {
		$out = $this->getOutput();
		$this->setHeaders();

		$bucket = $this->formData['bucket'] ?? '';
		$select = $this->formData['select'] ?? '*';
		$where = $this->formData['where'] ?? '';
		$limit = $this->formData['limit'] ?? 20;
		$offset = $this->formData['offset'] ?? 0;

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

		$fullResult = BucketPageHelper::runQuery( $this->getRequest(), $bucket, $select, $where, $limit, $offset );

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

		if ( $resultCount === 0 ) {
			return $out->addHTML( Html::noticeBox( $out->msg( 'bucket-empty-query' )->parse(), '' ) );
		}

		$pageLinks = BucketPageHelper::getPageLinks( $out, $this->getFullTitle(), $limit, $offset, null, $this->getRequest()->getQueryValues(), ( $resultCount === $limit ) );

		$out->addHTML( $pageLinks );
		$out->addWikiTextAsContent( BucketPageHelper::getResultTable( $schemas[$bucketName], $fullResult['fields'], $queryResult ) );
		$out->addHTML( $pageLinks );
	}

	function getGroupName() {
		return 'bucket';
	}
}
