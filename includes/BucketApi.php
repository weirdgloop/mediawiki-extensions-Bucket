<?php

namespace MediaWiki\Extension\Bucket;

use ApiBase;
use MediaWiki\Extension\Scribunto\Scribunto;
use MediaWiki\Extension\Scribunto\ScribuntoException;
use MediaWiki\MediaWikiServices;
use Parser;
use ParserOptions;
use Wikimedia\ParamValidator\ParamValidator;
use Wikimedia\ParamValidator\TypeDef\IntegerDef;

class BucketApi extends ApiBase {

	public function execute() {
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

			$dbw = MediaWikiServices::getInstance()->getDBLoadBalancer()->getConnectionRef( DB_PRIMARY );

			$res = $dbw->select( 'bucket_schemas', [ 'table_name', 'schema_json' ], [ 'table_name' => $bucket ] );
			// TODO: Early error return if bucket isn't valid
			$schemas = [];
			foreach ( $res as $row ) {
				$schemas[$row->table_name] = json_decode( $row->schema_json, true );
			}

			// Select everything if input is *
			$selectNames = [];
			if ( $select == '*' || $select == '' ) {
				foreach ( $schemas[$bucket] as $name => $value ) {
					if ( !str_starts_with( $name, '_' ) ) {
						$selectNames[] = $name;
					}
				}
			} else {
				$selectNames = explode( ' ', $select ); // TODO this breaks if given categories with spaces in them
			}
			$this->getResult()->addValue( null, 'columns', $selectNames );
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
		file_put_contents( MW_INSTALL_PATH . '/cook.txt', "$questionString\n", FILE_APPEND );

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
			// TODO this ends up being a message like "Lua error in mw.ext.bucket.lua at line 86: Bucket aaaaaa does not exist.." which is kinda uggo
			$this->getResult()->addValue( null, 'error', $e->getMessage() );
		}

		$this->getResult()->addValue( null, 'bucket', json_decode( $result['return'] ) );
	}

	protected function getAllowedParams() {
		return [
		'query' => [
			ParamValidator::PARAM_TYPE => 'string',
			ApiBase::PARAM_HELP_MSG => wfMessage( 'bucket-api-help-query' )
		],
		'bucket' => [
			ParamValidator::PARAM_TYPE => 'string',
			ApiBase::PARAM_HELP_MSG => wfMessage( 'bucket-api-help-bucket' )
		],
		'select' => [
			ParamValidator::PARAM_TYPE => 'string',
			ParamValidator::PARAM_DEFAULT => '*',
			ApiBase::PARAM_HELP_MSG => wfMessage( 'bucket-api-help-select' )
		],
		'where' => [
			ParamValidator::PARAM_TYPE => 'string',
			ApiBase::PARAM_HELP_MSG => wfMessage( 'bucket-api-help-where' )
		],
		'limit' => [
			ParamValidator::PARAM_DEFAULT => 20,
			ParamValidator::PARAM_TYPE => 'limit',
			IntegerDef::PARAM_MIN => 1,
			IntegerDef::PARAM_MAX => Bucket::DEFAULT_LIMIT,
			IntegerDef::PARAM_MAX2 => Bucket::MAX_LIMIT,
		],
		'offset' => [
			ParamValidator::PARAM_DEFAULT => 0,
			ParamValidator::PARAM_TYPE => 'limit',
			IntegerDef::PARAM_MIN => 0
		]
		];
	}

}
