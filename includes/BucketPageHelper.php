<?php

namespace MediaWiki\Extension\Bucket;

use MediaWiki\Request\DerivativeRequest;
use ApiMain;
use OOUI;

class BucketPageHelper {

    public static function runQuery($existing_request, $bucket, $select, $where, $limit, $offset) {
        $params = new DerivativeRequest(
            $existing_request,
            array(
                'action' => 'bucket',
                'bucket' => $bucket,
                'select' => $select,
                'where' => $where,
                'limit' => $limit,
                'offset' => $offset
            )
        );
        $api = new ApiMain($params);
        $api->execute();
        return $api->getResult()->getResultData();
    }

    public static function formatValue($value, $dataType, $repeated) {
        if ($repeated) {
            $json = json_decode($value);
            $returns = [];
            foreach ($json as $val) {
                $formatted_val = BucketPageHelper::formatValue( $val, $dataType, false);
                if ( $formatted_val != '' ) {
                    #TODO: Move this to a css file
                    $returns[] = '<li style="text-wrap-mode: nowrap;">' . $formatted_val;
                }
            }
            return implode('', $returns);
        }
        if ($dataType == "PAGE" && strlen($value) > 0) {
            return "[[$value]]";
        }
        if ($dataType == "TEXT") {
            return "<nowiki>$value</nowiki>";
        }
        return $value;
    }

    public static function getResultTable($schema, $columns, $result) {
        file_put_contents(MW_INSTALL_PATH . '/cook.txt', "RESULT TABLE " . print_r($columns, true) . "\n", FILE_APPEND);
        if (isset($columns) && count($columns) > 0) {
            $output[] = "<table class=\"wikitable\"><tr>";
            $keys = [];
            foreach (array_keys($schema) as $key) {
                if (in_array($key, $columns)) {
                    $keys[] = $key;
                    $output[] = "<th>$key</th>";
                }
            }
            foreach ($result as $row) {
                $output[] = "<tr>";
                foreach ($keys as $key) {
                    if (isset($row[$key])) {
                        $output[] = "<td>" . BucketPageHelper::formatValue($row[$key], $schema[$key]['type'], $schema[$key]['repeated']) . "</td>";
                    } else {
                        $output[] = "<td></td>";
                    }
                }
                $output[] = "</tr>";
            }
            $output[] = "</table>";
            return implode('', $output);
        }
        return '';
    }

    public static function getPageLinks($title, $limit, $offset, $query, $hasNext = true) {
        $links = [];

        $previousOffset = max(0, $offset-$limit);
        $links[] = new OOUI\ButtonWidget( [
            'href' => $title->getLocalURL( [ 'limit' => $limit, 'offset' => max(0, $previousOffset) ] + $query ),
            'title' => wfMessage("bucket-previous") . " $limit results.",
            'label' => wfMessage("bucket-previous") . " $limit",
            'disabled' => ($offset == 0)
        ]);

        foreach ( [20, 50, 100, 250, 500 ] as $num ) {
            $query = [ 'limit' => $num, 'offset' => $offset ] + $query;
            $tooltip = "Show $num results per page.";
            $links[] = new OOUI\ButtonWidget( [
                'href' => $title->getLocalURL($query),
                'title' => $tooltip,
                'label' => $num,
                'active' => ($num == $limit)
            ]);
        }

        $links[] = new OOUI\ButtonWidget( [
            'href' => $title->getLocalURL( [ 'limit' => $limit, 'offset' => $offset+$limit ] + $query ),
            'title' => wfMessage("bucket-next") . " $limit results.",
            'label' => wfMessage("bucket-next") . " $limit",
            'disabled' => !$hasNext
        ]);

        return new OOUI\ButtonGroupWidget( [ 'items' => $links ] );
    } 
}