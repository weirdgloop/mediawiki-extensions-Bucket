<?php

namespace MediaWiki\Extension\Bucket;

use MediaWiki\Api\ApiMain;
use MediaWiki\Html\Html;
use MediaWiki\Html\TemplateParser;
use MediaWiki\MediaWikiServices;
use MediaWiki\Request\DerivativeRequest;
use MediaWiki\Request\WebRequest;
use MediaWiki\Title\Title;
use MediaWiki\Title\TitleValue;
use OOUI;

class BucketPageHelper {
	/**
	 * @param WebRequest $existing_request
	 * @param string $bucket
	 * @param string $select
	 * @param string $where
	 * @param int $limit
	 * @param int $offset
	 * @return array
	 */
	public static function runQuery(
		WebRequest $existing_request, string $bucket, string $select, string $where, int $limit, int $offset
	): array {
		$params = new DerivativeRequest(
			$existing_request,
			[
				'action' => 'bucket',
				'bucket' => $bucket,
				'select' => $select,
				'where' => $where,
				'limit' => $limit,
				'offset' => $offset
			]
		);
		$api = new ApiMain( $params );
		$api->execute();
		return $api->getResult()->getResultData();
	}

	/**
	 * @return string - wikitext
	 */
	private static function formatValue( mixed $value, string $dataType, bool $repeated ): string {
		if ( $repeated ) {
			if ( !is_array( $value ) ) {
				$json = json_decode( $value );
			} else {
				$json = $value;
			}
			$returns = [];
			$returns[] = '<ul class="bucket__list">';
			foreach ( $json as $val ) {
				$formatted_val = self::formatValue( $val, $dataType, false );
				if ( $formatted_val !== '' ) {
					$returns[] = '<li class="bucket__list-item">' . $formatted_val . '</li>';
				}
			}
			$returns[] = '</ul>';
			return implode( '', $returns );
		}

		$class = 'bucket__value-' . strtolower( $dataType );

		if ( $dataType == 'PAGE' && strlen( $value ) > 0 ) {
			$renderer = MediaWikiServices::getInstance()->getLinkRenderer();
			return Html::rawElement(
				'div', [ 'class' => $class ], $renderer->makePreloadedLink( new TitleValue( 0, $value ) ) );
		} elseif ( $dataType == 'BOOLEAN' ) {
			$value = $value ? 'true' : 'false';
			return Html::element( 'span', [
				'class' => "$class bucket__value-boolean-$value" ], $value );
		} else {
			return Html::element( 'span', [ 'class' => $class ], $value );
		}
	}

	/**
	 * @param TemplateParser $templateParser
	 * @param array $schema
	 * @param array|null $fields
	 * @param array $result
	 * @return string - html
	 */
	public static function getResultTable(
		TemplateParser $templateParser, array $schema, ?array $fields, array $result ): string {
		if ( isset( $fields ) && count( $fields ) > 0 ) {
			$keys = [];
			$rows = [];

			foreach ( array_keys( $schema ) as $key ) {
				if ( in_array( $key, $fields ) ) {
					$keys[] = $key;
				}
			}

			foreach ( $result as $row ) {
				$tr = [];
				foreach ( $keys as $key ) {
					$tr[] = isset( $row[$key] ) ? self::formatValue(
						$row[$key], $schema[$key]['type'], $schema[$key]['repeated'] ) :
						'<span class="bucket__value-null">null</span>';
				}
				$rows[] = $tr;
			}

			return $templateParser->processTemplate(
				'BucketResultTable',
				[
					'keys' => $keys,
					'rows' => $rows
				]
			);
		}

		return '';
	}

	/**
	 * @param Title $title
	 * @param int $limit
	 * @param int $offset
	 * @param array $query
	 * @param bool $hasNext
	 * @return OOUI\ButtonGroupWidget
	 */
	public static function getPageLinks(
		Title $title, int $limit, int $offset, array $query, bool $hasNext = true
	): OOUI\ButtonGroupWidget {
		$links = [];

		$previousOffset = max( 0, $offset - $limit );
		$links[] = new OOUI\ButtonWidget( [
			'href' => $title->getLocalURL( [ 'limit' => $limit, 'offset' => max( 0, $previousOffset ) ] + $query ),
			'title' => wfMessage( 'bucket-previous-results', $limit ),
			'label' => wfMessage( 'bucket-previous' ) . " $limit",
			'disabled' => ( $offset === 0 )
		] );

		foreach ( [ 20, 50, 100, 250, 500 ] as $num ) {
			$links[] = new OOUI\ButtonWidget( [
				'href' => $title->getLocalURL( [ 'limit' => $num, 'offset' => $offset ] + $query ),
				'title' => wfMessage( 'bucket-results-per-page-tooltip', $num ),
				'label' => $num,
				'active' => ( $num === $limit )
			] );
		}

		$links[] = new OOUI\ButtonWidget( [
			'href' => $title->getLocalURL( [ 'limit' => $limit, 'offset' => $offset + $limit ] + $query ),
			'title' => wfMessage( 'bucket-next-results', $limit ),
			'label' => wfMessage( 'bucket-next' ) . " $limit",
			'disabled' => !$hasNext
		] );

		return new OOUI\ButtonGroupWidget( [ 'items' => $links ] );
	}

	/**
	 * Escapes input and wraps in a standard error format.
	 * @return string - escaped wikitext
	 */
	public static function printError( string $msg ) {
		return '<strong class="error bucket-error">' . wfEscapeWikiText( $msg ) . '</strong>';
	}
}
