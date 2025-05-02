<?php

namespace Mediawiki\Extension\Bucket;

use Article;
use Mediawiki\Title\Title;
use MediaWiki\MediaWikiServices;
use MediaWiki\Extension\Bucket\Bucket;
use MediaWiki\Extension\Bucket\BucketPageHelper;
use MediaWiki\Request\DerivativeRequest;
use OOUI;
use ApiMain;

class BucketPage extends Article {

    public function __construct( Title $title ) {
        parent::__construct( $title );
    }

    private function runQuery($bucket, $select, $where, $limit, $offset) {
        $params = new DerivativeRequest(
            $this->getContext()->getRequest(),
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

    private function formatValue($value, $dataType, $repeated) {
        if ($repeated) {
            $json = json_decode($value);
            $returns = [];
            foreach ($json as $val) {
                $formatted_val = $this->formatValue( $val, $dataType, false);
                if ( $formatted_val != '' ) {
                    $returns[] = '<li>' . $formatted_val;
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

    public function view() {
        parent::view();
        $context = $this->getContext();
        $out = $this->getContext()->getOutput();
        $out->enableOOUI();
        $out->addModuleStyles("ext.bucket.bucketpage.css");
        $out->disableClientCache(); //DEBUG
        $title = $this->getTitle();
        $out->setPageTitle( $title );

		$dbw = MediaWikiServices::getInstance()->getDBLoadBalancer()->getConnectionRef( DB_PRIMARY );

        $table_name = Bucket::getValidFieldName($title->getRootText());

        $res = $dbw->select( 'bucket_schemas', [ 'table_name', 'schema_json' ], [ 'table_name' => $table_name ] );
		$schemas = [];
		foreach ( $res as $row ) {
			$schemas[$row->table_name] = json_decode( $row->schema_json, true );
		}

        $select = $context->getRequest()->getText( "select", '*');
        $where = $context->getRequest()->getText( "where", '' );
        $limit = $context->getRequest()->getInt( "limit", 20 );
        $offset = $context->getRequest()->getInt( "offset", 0 );

        $fullResult = $this->runQuery($table_name, $select, $where, $limit, $offset);

        if (isset($fullResult['error'])) {
            $out->addHTML($fullResult['error']);
        }
        if (isset($fullResult['bucket'])) {
            $queryResult = $fullResult['bucket'];
        }


        $out->addHTML(BucketPageHelper::getPageLinks($title, $limit, $offset, $context->getRequest()->getQueryValues()));

        $output = [];

        if (isset($queryResult) && isset($queryResult[0])) {
            // file_put_contents(MW_INSTALL_PATH . '/cook.txt', "query results " . print_r($queryResult, true) . "\n", FILE_APPEND);
            $output[] = "<table class=\"wikitable\"><tr>";
            $keys = [];
            foreach (array_keys($schemas[$table_name]) as $key) {
                if (isset($queryResult[0][$key])) {
                    $keys[] = $key;
                    $output[] = "<th>$key</th>";
                }
            }
            foreach ($queryResult as $row) {
                $output[] = "<tr>";
                foreach ($keys as $key) {
                    $output[] = "<td>" . $this->formatValue($row[$key], $schemas[$table_name][$key]['type'], $schemas[$table_name][$key]['repeated']) . "</td>";
                }
                $output[] = "</tr>";
            }
            $output[] = "</table>";
        }

        $out->addWikiTextAsContent( implode('', $output ) );
    }
}