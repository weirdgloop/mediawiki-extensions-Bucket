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
        ->select(['table_name', 'schema_json'])
        ->where(['table_name' => $table_name])
        ->caller(__METHOD__)
        ->fetchResultSet();
		$schemas = [];
		foreach ( $res as $row ) {
			$schemas[$row->table_name] = json_decode( $row->schema_json, true );
		}

        $select = $context->getRequest()->getText( "select", '*');
        $where = $context->getRequest()->getText( "where", '' );
        $limit = $context->getRequest()->getInt( "limit", 20 );
        $offset = $context->getRequest()->getInt( "offset", 0 );

        $fullResult = BucketPageHelper::runQuery($this->getContext()->getRequest(), $table_name, $select, $where, $limit, $offset);

        if (isset($fullResult['error'])) {
            $out->addHTML($fullResult['error']);
        }
        if (isset($fullResult['bucket'])) {
            $queryResult = $fullResult['bucket'];
        }

        $resultCount = count($fullResult['bucket']);
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
        $out->addWikiTextAsContent( BucketPageHelper::getResultTable($schemas[$table_name], $fullResult['columns'], $queryResult) );
        $out->addHTML($pageLinks);
    }
}