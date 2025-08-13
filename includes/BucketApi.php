<?php

namespace MediaWiki\Extension\Bucket;

use MediaWiki\Api\ApiBase;
use MediaWiki\Api\ApiMain;
use MediaWiki\Extension\Scribunto\Scribunto;
use MediaWiki\Extension\Scribunto\ScribuntoException;
use MediaWiki\MediaWikiServices;
use MediaWiki\Parser\Parser;
use MediaWiki\Parser\ParserOptions;
use Wikimedia\ParamValidator\ParamValidator;
use Wikimedia\ParamValidator\TypeDef\NumericDef;

class BucketApi extends ApiBase {
	private BucketDatabase $bucketDb;

	public function __construct( ApiMain $mainModule, string $moduleName, BucketDatabase $bucketDb ) {
		parent::__construct( $mainModule, $moduleName );
		$this->bucketDb = $bucketDb;
	}

	public function execute() {
		if ( $this->getUser()->pingLimiter( 'bucketapi', 1 ) ) {
			return;
		}

		$params = $this->extractRequestParams();

		$title = $this->getTitle();

		// The query param is a fully built lua string
		if ( isset( $params['query'] ) ) {
			$questionString = '= mw.text.jsonEncode(' . $params['query'] . ')';
		} else {
			$bucket = $params['bucket'];
			$select = $params['select'];
			$where = $params['where'];
			$limit = $params['limit'];
			$offset = $params['offset'];

			if ( $bucket === null ) {
				$this->getResult()->addValue( null, 'error', $this->msg( 'bucket-empty-bucket-name' ) );
				return;
			}
			try {
				$bucket = Bucket::getValidBucketName( $bucket );
			} catch ( SchemaException $e ) {
				$this->getResult()->addValue( null, 'error', $e->getMessage() );
				return;
			}

			// Select everything if input is *
			$selectNames = [];
			if ( $select === '*' || $select === '' ) {
				$dbw = $this->bucketDb->getDB();
				$res = $dbw->newSelectQueryBuilder()
					->from( 'bucket_schemas' )
					->select( [ 'bucket_name', 'schema_json' ] )
					->where( [ 'bucket_name' => $bucket ] )
					->caller( __METHOD__ )
					->fetchResultSet();
				$schemas = [];
				foreach ( $res as $row ) {
					$schemas[$row->bucket_name] = json_decode( $row->schema_json, true );
				}
				if ( isset( $schemas[$bucket] ) ) {
					foreach ( $schemas[$bucket] as $name => $value ) {
						if ( !str_starts_with( $name, '_' ) ) {
							$selectNames[] = $name;
						}
					}
				}
			} else {
				$selectNames = explode( ' ', $select );
			}
			$this->getResult()->addValue( null, 'fields', $selectNames );
			foreach ( $selectNames as $idx => $name ) {
				$selectNames[$idx] = "'" . $name . "'";
			}
			$select = implode( ',', $selectNames );

			$questionString = [];
			$questionString[] = "= mw.text.jsonEncode(bucket('$bucket')";
			$questionString[] = ".select($select)";
			if ( strlen( $where ) > 0 ) {
				$questionString[] = ".where($where)";
			}
			$questionString[] = ".limit($limit).offset($offset).run())";
			$questionString = implode( '', $questionString );
		}

		$this->getResult()->addValue( null, 'bucketQuery', $questionString );

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

		$this->getResult()->addValue( null, 'bucket', json_decode( $result['return'] ) );
	}

	/**
	 * @return array[]
	 */
	protected function getAllowedParams() {
		return [
			'query' => [
				ParamValidator::PARAM_TYPE => 'string',
				ApiBase::PARAM_HELP_MSG => $this->msg( 'bucket-api-help-query' )
			],
			'bucket' => [
				ParamValidator::PARAM_TYPE => 'string',
				ApiBase::PARAM_HELP_MSG => $this->msg( 'bucket-api-help-bucket' )
			],
			'select' => [
				ParamValidator::PARAM_TYPE => 'string',
				ParamValidator::PARAM_DEFAULT => '*',
				ApiBase::PARAM_HELP_MSG => $this->msg( 'bucket-api-help-select' )
			],
			'where' => [
				ParamValidator::PARAM_TYPE => 'string',
				ApiBase::PARAM_HELP_MSG => $this->msg( 'bucket-api-help-where' )
			],
			'limit' => [
				ParamValidator::PARAM_DEFAULT => 20,
				ParamValidator::PARAM_TYPE => 'limit',
				NumericDef::PARAM_MIN => 1,
				NumericDef::PARAM_MAX => BucketQuery::DEFAULT_LIMIT,
				NumericDef::PARAM_MAX2 => BucketQuery::MAX_LIMIT,
			],
			'offset' => [
				ParamValidator::PARAM_DEFAULT => 0,
				ParamValidator::PARAM_TYPE => 'limit',
				NumericDef::PARAM_MIN => 0
			]
		];
	}

}
