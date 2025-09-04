<?php

namespace MediaWiki\Extension\Bucket;

use MediaWiki\Api\ApiBase;
use MediaWiki\Extension\Scribunto\Scribunto;
use MediaWiki\Extension\Scribunto\ScribuntoException;
use MediaWiki\MediaWikiServices;
use MediaWiki\Parser\Parser;
use MediaWiki\Parser\ParserOptions;
use Wikimedia\ParamValidator\ParamValidator;

class BucketApi extends ApiBase {

	public function execute() {
		if ( $this->getUser()->pingLimiter( 'bucketapi', 1 ) ) {
			return;
		}

		$params = $this->extractRequestParams();

		$title = $this->getTitle();

		// The query param is a fully built lua string
		if ( isset( $params['query'] ) ) {
			$this->getResult()->addValue( null, 'bucketQuery', $params['query'] );
			$questionString = '= mw.text.jsonEncode(' . $params['query'] . ')';
		} else {
			$this->getResult()->addValue( null, 'error', 'query parameter is required' );
			return;
		}

		$parser = MediaWikiServices::getInstance()->getParser();
		$options = new ParserOptions( $this->getUser() );
		$parser->startExternalParse( $title, $options, Parser::OT_HTML, true );
		$engine = Scribunto::getParserEngine( $parser );
		try {
			$result = $engine->runConsole( [
				'title' => $title,
				'content' => '',
				'prevQuestions' => [],
				'question' => $questionString
			] );

		} catch ( ScribuntoException $e ) {
			$errorMsg = $e->getMessage();
			// Remove the "Lua error in mw.ext.bucket.ua at line 85" text to clean up the error a little bit.
			$errorMsg = preg_replace( '/Lua error in .+: /U', '', $errorMsg );
			$this->getResult()->addValue( null, 'error', $errorMsg );
			return;
		}

		$res = json_decode( $result['return'] );
		foreach ( $res as $key => $value ) {
			if ( is_array( $value ) && empty( $value ) ) {
					$res[$key] = new \stdClass();
			}
		}
		$this->getResult()->addValue( null, 'bucket', $res );
	}

	/**
	 * @return array[]
	 */
	protected function getAllowedParams() {
		return [
			'query' => [
				ParamValidator::PARAM_TYPE => 'string',
				ApiBase::PARAM_HELP_MSG => $this->msg( 'bucket-api-help-query' ),
				ParamValidator::PARAM_REQUIRED => true
			]
		];
	}

}
