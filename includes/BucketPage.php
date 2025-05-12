<?php

namespace Mediawiki\Extension\Bucket;

use Article;
use MediaWiki\Title\TitleValue;
use Mediawiki\Title\Title;
use MediaWiki\MediaWikiServices;
use MediaWiki\Extension\Bucket\Bucket;
use MediaWiki\Extension\Bucket\BucketPageHelper;

class BucketPage extends Article {

    public function __construct( Title $title ) {
        parent::__construct( $title );
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

        $res = $dbw->newSelectQueryBuilder()
                    ->from('bucket_schemas')
                    ->select(['table_name', 'backing_table_name', 'schema_json'])
                    ->where($dbw->makeList(['table_name' => $table_name, 'backing_table_name' => $table_name], LIST_OR))
                    ->caller(__METHOD__)
                    ->fetchResultSet();
		$schemas = [];
		$backingBucketName = [];
		foreach ( $res as $row ) {
			$schemas[$row->table_name] = json_decode( $row->schema_json, true );
			$backingBucketName[$row->table_name] = $row->backing_table_name;
            //Buckets pointing to this bucket
            if ($row->table_name != $table_name) {
                //TODO views are never deleted so this warning is kinda meh
                $out->addHTML("<h3>Warning: Bucket $row->table_name points to this Bucket.</h3>");
            }
		}

        $select = $context->getRequest()->getText( "select", '*');
        $where = $context->getRequest()->getText( "where", '' );
        $limit = $context->getRequest()->getInt( "limit", 20 );
        $offset = $context->getRequest()->getInt( "offset", 0 );

        $fullResult = BucketPageHelper::runQuery($this->getContext()->getRequest(), $table_name, $select, $where, $limit, $offset);

        if (isset($fullResult['error'])) {
            file_put_contents(MW_INSTALL_PATH . '/cook.txt', "ERROR " . print_r($fullResult['error'], true) . "\n", FILE_APPEND);
            $out->addHTML($fullResult['error']);
            return;
        }
        $queryResult = [];
        if (isset($fullResult['bucket'])) {
            $queryResult = $fullResult['bucket'];
        }

        if ($backingBucketName[$table_name] !== null) {
            $out->addWikiTextAsContent("<h3>Warning: This Bucket redirects to [[Bucket:$backingBucketName[$table_name]]]. All puts should be updated to use the new name.</h3>");
        }

        $resultCount = count($queryResult);
        $endResult = $offset + $resultCount;
        $out->addHTML("Displaying $resultCount results $offset – $endResult. ");

        $linkRenderer = MediaWikiServices::getInstance()->getLinkRenderer();
        $specialQueryValues = $context->getRequest()->getQueryValues();
        unset($specialQueryValues['action']);
        unset($specialQueryValues['title']);
        $specialQueryValues['bucket'] = $table_name;
        $out->addHTML($linkRenderer->makeLink(new TitleValue(NS_SPECIAL, "Bucket"), "Dive into this Bucket", [], $specialQueryValues));
        $out->addHTML('<br>');

        $pageLinks = BucketPageHelper::getPageLinks($title, $limit, $offset, $context->getRequest()->getQueryValues(), ($resultCount == $limit));

        $out->addHTML($pageLinks);
        $out->addWikiTextAsContent(BucketPageHelper::getResultTable($schemas[$table_name], $fullResult['columns'], $queryResult));
        $out->addHTML($pageLinks);
    }
}