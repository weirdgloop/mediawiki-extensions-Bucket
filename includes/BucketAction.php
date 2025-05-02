<?php

use MediaWiki\MediaWikiServices;
use MediaWiki\Extension\Bucket\BucketPageHelper;

class BucketAction extends Action {

    public function getName() {
        return "bucket";
    }

    public function show() {
        $this->getOutput()->enableOOUI(); //We want to use OOUI for consistent styling
        if ($this->getArticle()->getTitle()->inNamespaces(9592, 9593)) {
            // return $this->showBucketNamespace();
            //TODO redirect to special bucket with the bucket name filled in
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
                        $output[] = "<td>" . BucketPageHelper::formatValue($value, $schemas[$table_name][$key]['type'], $schemas[$table_name][$key]['repeated']) . "</td>";
                    }
                }
                $output[] = "</tr>";
            }
            $output[] = "</table>";
        }

        $out->addWikiTextAsContent( implode('', $output ) );
    }

}