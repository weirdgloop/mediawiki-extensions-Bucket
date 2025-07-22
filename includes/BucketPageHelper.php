<?php

namespace MediaWiki\Extension\Bucket;

use MediaWiki\Api\ApiMain;
use MediaWiki\Navigation\PagerNavigationBuilder;
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
				if ( $formatted_val != '' ) {
					$returns[] = '<li class="bucket-list">' . $formatted_val;
				}
			}
			return implode( '', $returns );
		}
		if ( $dataType == 'PAGE' && strlen( $value ) > 0 ) {
			return '[[:' . wfEscapeWikiText( $value ) . ']]';
		}
		if ( $dataType == 'TEXT' ) {
			return wfEscapeWikiText( $value );
		}
		if ( $dataType == 'BOOLEAN' ) {
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

	public static function getPageLinks(
		$messageLocalizer, $title, $limit, $offset, $query, $hasNext = true
	) {
		$navBuilder = new PagerNavigationBuilder( $messageLocalizer );
		$navBuilder
			->setPage( $title )
			->setLinkQuery( [ 'limit' => $limit, 'offset' => $offset ] + $query )
			->setLimitLinkQueryParam( 'limit' )
			->setCurrentLimit( $limit )
			->setPrevTooltipMsg( 'prevn-title' )
			->setNextTooltipMsg( 'nextn-title' )
			->setLimitTooltipMsg( 'shown-title' );

		if ( $offset > 0 ) {
			$navBuilder->setPrevLinkQuery( [ 'offset' => (string)max( $offset - $limit, 0 ) ] );
		}
		if ( $hasNext ) {
			$navBuilder->setNextLinkQuery( [ 'offset' => (string)( $offset + $limit ) ] );
		}

		return $navBuilder->getHtml();
	}

	/**
	 * Escapes input and wraps in a standard error format.
	 */
	public static function printError( string $msg ) {
		return '<strong class="error bucket-error">' . wfEscapeWikiText( $msg ) . '</strong>';
	}
}
