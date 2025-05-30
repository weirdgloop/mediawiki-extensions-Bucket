<?php

namespace MediaWiki\Extension\Bucket;

use ApiMain;
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

	public static function formatValue( $value, $dataType, $repeated ) {
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
			return "[[$value]]";
		}
		if ( $dataType == 'TEXT' ) {
			return "<nowiki>$value</nowiki>";
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

	public static function getResultTable( $schema, $columns, $result ) {
		if ( isset( $columns ) && count( $columns ) > 0 ) {
			$output[] = '<table class="wikitable"><tr>';
			$keys = [];
			foreach ( array_keys( $schema ) as $key ) {
				if ( in_array( $key, $columns ) ) {
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

	public static function getPageLinks( $title, $limit, $offset, $query, $hasNext = true ) {
		$links = [];

		$previousOffset = max( 0, $offset - $limit );
		$links[] = new OOUI\ButtonWidget( [
			'href' => $title->getLocalURL( [ 'limit' => $limit, 'offset' => max( 0, $previousOffset ) ] + $query ),
			'title' => wfMessage( 'bucket-previous-results', $limit ),
			'label' => wfMessage( 'bucket-previous' ) . " $limit",
			'disabled' => ( $offset == 0 )
		] );

		foreach ( [ 20, 50, 100, 250, 500 ] as $num ) {
			$query = [ 'limit' => $num, 'offset' => $offset ] + $query;
			$tooltip = "Show $num results per page.";
			$links[] = new OOUI\ButtonWidget( [
				'href' => $title->getLocalURL( $query ),
				'title' => $tooltip,
				'label' => $num,
				'active' => ( $num == $limit )
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
}
