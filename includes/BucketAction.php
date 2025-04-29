<?php

use MediaWiki\MediaWikiServices;
use MediaWiki\Extension\Bucket\Bucket;

class BucketAction extends Action {

    public function getName() {
        return "bucket";
    }

    private function formatValue($value, $dataType, $repeated) {
        if ($repeated) {
            $json = json_decode($value);
            $returns = [];
            foreach ($json as $val) {
                $returns[] = $this->formatValue($val, $dataType, false);
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

        $table_name = Bucket::getValidFieldName($title->getBaseText());

        $res = $dbw->select( 'bucket_schemas', [ 'table_name', 'schema_json' ], [ 'table_name' => $table_name ] );
		$schemas = [];
		foreach ( $res as $row ) {
			$schemas[$row->table_name] = json_decode( $row->schema_json, true );
		}

        $dissallowColumns = ['_page_id', 'page_name_version'];

        $output = [];
        #TODO allow pagination
        $res = $dbw->select('bucket__' . $table_name, ["*"], [], '', ['LIMIT' => 50]);
        $fieldNames = $res->getFieldNames();
        $output[] = "<table class=\"wikitable\"><tr>";
        foreach ($fieldNames as $name) {
            if (!in_array($name, $dissallowColumns)) {
                $output[] = "<th>$name</th>";
            }
        }
        foreach ($res as $row) {
            $output[] = "<tr>";
            foreach ($row as $key => $value) {
                if (!in_array($key, $dissallowColumns)) {
                    $output[] = "<td>" . $this->formatValue($value, $schemas[$table_name][$key]['type'], $schemas[$table_name][$key]['repeated']) . "</td>";
                }
            }
            $output[] = "</tr>";
        }
        $output[] = "</table>";

        $out->addWikiTextAsContent( implode('', $output ) );
    }

    public function show() {
        if ($this->getArticle()->getTitle()->inNamespaces(9592, 9593)) {
            return $this->showBucketNamespace();
        }

        $out = $this->getOutput();
        $title = $this->getArticle()->getTitle();
        $pageId = $this->getArticle()->getPage()->getId();

		$dbw = MediaWikiServices::getInstance()->getDBLoadBalancer()->getConnectionRef( DB_PRIMARY );

        $res = $dbw->select('bucket_pages', ['table_name'], ["_page_id" => $pageId]);
        $tables = [];
        foreach ( $res as $row ) {
            $tables[] = $row->table_name;
        }

        $res = $dbw->select( 'bucket_schemas', [ 'table_name', 'schema_json' ], [ 'table_name' => $tables ] );
		$schemas = [];
		foreach ( $res as $row ) {
			$schemas[$row->table_name] = json_decode( $row->schema_json, true );
		}

        $dissallowColumns = ['_page_id', 'page_name', 'page_name_version'];

        $out->setPageTitle( "Bucket View: $title" );
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