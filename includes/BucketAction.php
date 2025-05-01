<?php

use MediaWiki\MediaWikiServices;
use MediaWiki\Extension\Bucket\Bucket;
use MediaWiki\Request\DerivativeRequest;

class BucketAction extends Action {

    public function getName() {
        return "bucket";
    }

    private function getPageLinks($limit, $offset, $query) {
        //TODO localized language
        $links = [];

        $previousOffset = max(0, $offset-$limit);
        $links[] = new OOUI\ButtonWidget( [
            'href' => $this->getTitle()->getLocalURL( [ 'action' => 'bucket', 'limit' => $limit, 'offset' => max(0, $previousOffset) ] + $query ),
            'title' => "Previous $limit results.",
            'label' => "Previous $limit"
        ]);
        if ( $offset-$limit < 0 ) {
            end($links)->setDisabled(true);
        }

        foreach ( [20, 50, 100, 250, 500 ] as $num ) {
            $query = [ 'action' => 'bucket', 'limit' => $num, 'offset' => $offset ] + $query;
            $tooltip = "Show $num results per page.";
            $links[] = new OOUI\ButtonWidget( [
                'href' => $this->getTitle()->getLocalURL($query),
                'title' => $tooltip,
                'label' => $num,
                'active' => ($num == $limit)
            ]);
        }

        $links[] = new OOUI\ButtonWidget( [
            'href' => $this->getTitle()->getLocalURL( [ 'action' => 'bucket', 'limit' => $limit, 'offset' => $offset+$limit ] + $query ),
            'title' => "Next $limit results.",
            'label' => "Next $limit"
        ]);

        return new OOUI\ButtonGroupWidget( [ 'items' => $links ] );
    }

    private function getQueryBuilder($lastQuery, $bucket, $select, $where, $limit, $offset) {
        $inputs = [];
        $inputs[] = new OOUI\HiddenInputWidget([
            "name" => "action",
            "value" => "bucket"
        ]);
        $inputs[] = new OOUI\HiddenInputWidget([
            "name" => "title",
            "value" => $this->getRequest()->getText("title")
        ]);
        $inputs[] = new OOUI\FieldLayout(
            new OOUI\TextInputWidget(
                [
                    "name" => "bucket",
                    "value" => $bucket,
                    "readOnly" => true
                ]
            ),
            [
                "align" => "right",
                "label" => "Bucket",
                "help" => "The current bucket name"
            ]
        );
        $inputs[] = new OOUI\FieldLayout(
            new OOUI\TextInputWidget(
                [
                    "name" => "select",
                    "value" => $select,
                ]
            ),
            [
                "align" => "right",
                "label" => "Select",
                "help" => "Names of columns to select, in quotes and seperated by commas. Such as 'page_name','uses_material' "
            ]
        );
        $inputs[] = new OOUI\FieldLayout(
            new OOUI\TextInputWidget(
                [
                    "name" => "where",
                    "value" => $where,
                ]
            ),
            [
                "align" => "right",
                "label" => "Where",
                "help" => "A valid lua string to be run inside bucket.where()"
            ]
        );
        $inputs[] = new OOUI\FieldLayout(
            new OOUI\NumberInputWidget(
                [
                    "name" => "limit",
                    "value" => $limit,
                    "min" => 0,
                    "max" => 500
                ]
            ),
            [
                "align" => "right",
                "label" => "Limit",
                "help" => "The number of results to return per page. Maximum 500."
            ]
        );
        $inputs[] = new OOUI\FieldLayout(
            new OOUI\NumberInputWidget(
                [
                    "name" => "offset",
                    "value" => $offset,
                    "min" => 0
                ]
            ),
            [
                "align" => "right",
                "label" => "Offset",
                "help" => "How far to offset the returned values."
            ]
        );
        $inputs[] = new OOUI\FieldLayout(
            new OOUI\ButtonInputWidget(
                [
                    "type" => "submit",
                    "label" => "Submit",
                    "align" => "center"

                ]),
                [
                    "label" => " "
                ]
        );

        $form = new OOUI\FormLayout([
            "items" => $inputs,
            "action" => '/',
            "method" => 'get'
        ]);

        return $form . "<br>";
    }

    private function runQuery($bucket, $select, $where, $limit, $offset) {
        $params = new DerivativeRequest(
            $this->getRequest(),
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

    private function showBucketNamespace() {
        $out = $this->getOutput();
        $title = $this->getArticle()->getTitle();
        $out->setPageTitle( "Bucket View: $title" );

		$dbw = MediaWikiServices::getInstance()->getDBLoadBalancer()->getConnectionRef( DB_PRIMARY );

        $table_name = Bucket::getValidFieldName($title->getRootText());

        $res = $dbw->select( 'bucket_schemas', [ 'table_name', 'schema_json' ], [ 'table_name' => $table_name ] );
		$schemas = [];
		foreach ( $res as $row ) {
			$schemas[$row->table_name] = json_decode( $row->schema_json, true );
		}

        $select = $this->getRequest()->getText( "select", '*');
        $where = $this->getRequest()->getText( "where", '' );
        $limit = $this->getRequest()->getInt( "limit", 50 );
        $offset = $this->getRequest()->getInt( "offset", 0 );

        $fullResult = $this->runQuery($table_name, $select, $where, $limit, $offset);

        if (isset($fullResult['error'])) {
            $out->addHTML($fullResult['error']);
        }
        if (isset($fullResult['bucket'])) {
            $queryResult = $fullResult['bucket'];
        }

        $out->addHTML($this->getQueryBuilder($fullResult['bucketQuery'], $table_name, $select, $where, $limit, $offset));

        $out->addHTML($this->getPageLinks($limit, $offset, $this->getRequest()->getQueryValues()));

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

    public function show() {
        $this->getOutput()->enableOOUI(); //We want to use OOUI for consistent styling
        if ($this->getArticle()->getTitle()->inNamespaces(9592, 9593)) {
            return $this->showBucketNamespace();
        }

        $out = $this->getOutput();
        $title = $this->getArticle()->getTitle();
        $pageId = $this->getArticle()->getPage()->getId();
        $out->setPageTitle( "Bucket View: $title" );

		$dbw = MediaWikiServices::getInstance()->getDBLoadBalancer()->getConnectionRef( DB_PRIMARY );

        $res =  $dbw->newSelectQueryBuilder()
            ->from('bucket_pages')
            ->select(['table_name'])
            ->where(['_page_id' => $pageId])
            ->groupBy('table_name')
            ->caller(__METHOD__)
            ->fetchResultSet();
        $tables = [];
        foreach ( $res as $row ) {
            $tables[] = $row->table_name;
        }

        if (count($tables) == 0) {
            $out->addWikiTextAsContent("No Buckets are written to from this page.");
            return;
        }

        $res = $dbw->newSelectQueryBuilder()
            ->from('bucket_schemas')
            ->select(['table_name', 'schema_json'])
            ->where(['table_name' => $tables])
            ->caller(__METHOD__)
            ->fetchResultSet();
		$schemas = [];
		foreach ( $res as $row ) {
			$schemas[$row->table_name] = json_decode( $row->schema_json, true );
		}

        $dissallowColumns = ['_page_id', 'page_name', 'page_name_version'];

        $output = [];
        foreach ( $tables as $table_name ) {
            $bucket_page_name = str_replace("_", " ", $table_name);
            $output[] = "<h2>[[Bucket:$bucket_page_name]]</h2>";
            $res = $dbw->select('bucket__' . $table_name, ["*"], ["_page_id" => $pageId]);
            $fieldNames = $res->getFieldNames();
            $output[] = "<table class=\"wikitable\"><tr>";
            foreach ( $fieldNames as $name ) {
                if (!in_array($name, $dissallowColumns)) {
                    $output[] = "<th>$name</th>";
                }
            }
            foreach ( $res as $row ) {
                $output[] = "<tr>";
                foreach ( $row as $key => $value ) {
                    if (!in_array($key, $dissallowColumns)) {
                        $output[] = "<td>" . $this->formatValue($value, $schemas[$table_name][$key]['type'], $schemas[$table_name][$key]['repeated']) . "</td>";
                    }
                }
                $output[] = "</tr>";
            }
            $output[] = "</table>";
        }

        $out->addWikiTextAsContent( implode('', $output ) );
    }

}