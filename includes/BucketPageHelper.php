<?php

namespace MediaWiki\Extension\Bucket;

use MediaWiki\Api\ApiMain;
use MediaWiki\Html\Html;
use MediaWiki\Request\DerivativeRequest;
use OOUI;

class BucketPageHelper {

	public static function runQuery( $existing_request, $bucket, $select, $where, $limit, $offset ) {
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

	private static function formatValue( mixed $value, string $dataType, bool $repeated ): string {
		if ( $repeated ) {
			if ( !is_array( $value ) ) {
				$json = json_decode( $value );
			} else {
				$json = $value;
			}
			$returns = [];
			foreach ( $json as $val ) {
				$formatted_val = self::formatValue( $val, $dataType, false );
				if ( $formatted_val !== '' ) {
					$returns[] = '<li class="bucket-list">' . $formatted_val;
				}
			}
			return implode( '', $returns );
		}
		if ( $dataType === 'PAGE' && strlen( $value ) > 0 ) {
			return '[[:' . wfEscapeWikiText( $value ) . ']]';
		}
		if ( $dataType === 'TEXT' ) {
			return wfEscapeWikiText( $value );
		}
		if ( $dataType === 'BOOLEAN' ) {
			if ( $value ) {
				return 'True';
			} else {
				return 'False';
			}
		}
		return $value;
	}

	public static function getResultTable( $schema, $fields, $result ) {
		if ( isset( $fields ) && count( $fields ) > 0 ) {
			$output[] = '<table class="wikitable"><tr>';
			$keys = [];
			foreach ( array_keys( $schema ) as $key ) {
				if ( in_array( $key, $fields ) ) {
					$keys[] = $key;
					$output[] = "<th>$key</th>";
				}
			}
			foreach ( $result as $row ) {
				$output[] = '<tr>';
				foreach ( $keys as $key ) {
					if ( isset( $row[$key] ) ) {
						$output[] = '<td>' . self::formatValue( $row[$key], $schema[$key]['type'], $schema[$key]['repeated'] ) . '</td>';
					} else {
						$output[] = '<td></td>';
					}
				}
				$output[] = '</tr>';
			}
			$output[] = '</table>';
			return implode( '', $output );
		}
		return '';
	}

	/**
	 * @param \MediaWiki\Output\OutputPage $out
	 * @param \MediaWiki\Title\Title $title
	 * @param int $limit
	 * @param int $offset
	 * @param int|null $totalResults
	 * @param array $query
	 * @param bool $hasNext
	 * @return string HTML for pagination links
	 */
	public static function getPageLinks(
		$out, $title, $limit, $offset, $totalResults, $query, $hasNext = true
	) {
		$out->enableOOUI();
		$out->addModuleStyles( [
			'oojs-ui.styles.icons-movement',
			'ext.bucket.view.styles'
		] );

		$limits = [];
		foreach ( [ 20, 50, 100, 250, 500 ] as $num ) {
			$query = [ 'limit' => $num, 'offset' => $offset ] + $query;
			$tooltip = $out->msg( 'bucket-limit-tooltip' )->numParams( $num )->parse();
			$limits[] = new OOUI\ButtonWidget( [
				'href' => $title->getLocalURL( $query ),
				'title' => $tooltip,
				'label' => $num,
				'active' => ( $num === $limit )
			] );
		}

		if ( $totalResults !== null ) {
			$resultsMessage = $out->msg( 'bucket-pagination-results' )
				->numParams( $offset + 1, $offset + $limit, $totalResults )
				->parse();
		} else {
			$resultsMessage = $out->msg( 'bucket-pagination-results-nototal' )
				->numParams( $offset + 1, $offset + $limit )
				->parse();
		}


		$previousOffset = max( 0, $offset - $limit );
		$prev = new OOUI\ButtonWidget( [
			'flags' => [ 'progressive' ],
			'framed' => false,
			'href' => $title->getLocalURL( [ 'limit' => $limit, 'offset' => max( 0, $previousOffset ) ] + $query ),
			'title' => wfMessage( 'bucket-previous-results' )->numParams( $limit )->parse(),
			'label' => wfMessage( 'bucket-previous-results' )->numParams( $limit )->parse(),
			'invisibleLabel' => true,
			'icon' => 'previous',
			'disabled' => ( $offset === 0 )
		] );

		$next = new OOUI\ButtonWidget( [
			'flags' => [ 'progressive' ],
			'framed' => false,
			'href' => $title->getLocalURL( [ 'limit' => $limit, 'offset' => $offset + $limit ] + $query ),
			'title' => wfMessage( 'bucket-next-results' )->numParams( $limit )->parse(),
			'label' => wfMessage( 'bucket-next-results' )->numParams( $limit )->parse(),
			'invisibleLabel' => true,
			'icon' => 'next',
			'disabled' => !$hasNext
		] );

		return Html::rawElement( 'div', [ 'class' => 'bucket-pagination' ],
			Html::rawElement( 'div', [ 'class' => 'bucket-pagination-message' ], $resultsMessage ) .
			Html::rawElement( 'div', [ 'class' => 'bucket-pagination-limit' ],
				$out->msg( 'bucket-pagination-limit' )->parse() .
				new OOUI\ButtonGroupWidget( [
					'items' => $limits
				] )
			) .
			Html::rawElement( 'div', [ 'class' => 'bucket-pagination-buttons' ],
				new OOUI\ButtonGroupWidget( [
					'items' => [ $prev, $next ]
				] )
			)
		);
	}

	/**
	 * Escapes input and wraps in a standard error format.
	 */
	public static function printError( string $msg ) {
		return '<strong class="error bucket-error">' . wfEscapeWikiText( $msg ) . '</strong>';
	}
}
