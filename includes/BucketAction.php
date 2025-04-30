<?php

use MediaWiki\MediaWikiServices;
use MediaWiki\Extension\Bucket\Bucket;
use MediaWiki\Extension\Scribunto\Scribunto;
use MediaWiki\Extension\Scribunto\ScribuntoException;

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

    private function runQuery($bucket, $select, $where, $limit, $offset) {
        $title = $this->getArticle()->getTitle();

        // $questionString = "= bucket($bucket).select($select).limit($limit).offset($offset).runJson()";
        $questionString = [];
        $questionString[] = "= bucket('$bucket')";
        $questionString[] = ".select($select)";
        if ( strlen($where) > 0 ) {
            $questionString[] = ".where($where)";
        }
        $questionString[] = ".limit($limit).offset($offset).runJson()";
        $questionString = implode('', $questionString);
		file_put_contents(MW_INSTALL_PATH . '/cook.txt', "$questionString\n", FILE_APPEND);

        // $oouiGroup = new OOUI\FormLayout();
        // $oouiInputs = [];
        // $oouiInputs[] = new OOUI\NumberInputWidget(['min' => 0]);
        // $oouiInputs[] = new OOUI\NumberInputWidget(['min' => 1, 'max' => 500]);

        // $oouiGroup->addItems($oouiInputs);
        // $oouiInput = new OOUI\MultilineTextInputWidget();
        // $oouiInput->setValue("= bucket($bucket).select($select).where($where).limit($limit).offset($offset).runJson()");
        // $oouiGroup->appendContent($oouiInput);

        $parser = MediaWikiServices::getInstance()->getParser();
        $options = new ParserOptions($this->getUser());
        $parser->startExternalParse($title, $options, Parser::OT_HTML, true);
        $engine = Scribunto::getParserEngine($parser);
        try {
            $result = $engine->runConsole([
                'title' => $title,
                'content' => '',
                'prevQuestions' => [],
                'question' => $questionString
            ]);

        } catch (ScribuntoException $e) {
            return $e;
        }
        return json_decode($result["return"]);
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

        //If select isn't specified, then select everything
        $selectNames = [];
        if ( $this->getRequest()->getText( "select" ) == '' ) {
            foreach ( $schemas[ $table_name ] as $name => $value ) {
                if ( !str_starts_with($name, '_' ) ) {
                    $selectNames[] = "'" . $name . "'";
                }
            }
        }

        $select = $this->getRequest()->getText( "select", implode(',', $selectNames));
        $where = $this->getRequest()->getText( "where", '' );
        $limit = $this->getRequest()->getInt( "limit", 50 );
        $offset = $this->getRequest()->getInt( "offset", 0 );

        $out->addHTML($this->getPageLinks($limit, $offset, $this->getRequest()->getQueryValues()));

        $queryResult = $this->runQuery($table_name, $select, $where, $limit, $offset);

        $output = [];

        $output[] = "<table class=\"wikitable\"><tr>";
        foreach ($selectNames as $name) {
            $output[] = "<th>$name</th>";
        }
        foreach ($queryResult as $row) {
            $output[] = "<tr>";
            foreach ($row as $key => $value) {
                $output[] = "<td>" . $this->formatValue($value, $schemas[$table_name][$key]['type'], $schemas[$table_name][$key]['repeated']) . "</td>";
            }
            $output[] = "</tr>";
        }
        $output[] = "</table>";

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