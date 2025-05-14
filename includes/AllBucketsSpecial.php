<?php

namespace MediaWiki\Extension\Bucket;

use MediaWiki\SpecialPage\SpecialPage;
use MediaWiki\MediaWikiServices;

class AllBucketsSpecial extends SpecialPage {
    public function __construct() {
        parent::__construct( 'allbuckets' );
    }

    public function execute( $par ) {
		$out = $this->getOutput();
		$this->setHeaders();

        $out->setPageTitle( wfMessage("bucket-specialpage-all-buckets-title") );

        $dbw = MediaWikiServices::getInstance()->getDBLoadBalancer()->getConnectionRef( DB_PRIMARY );
        $res = $dbw->newSelectQueryBuilder()
            ->from('bucket_schemas')
            ->select(['table_name', 'backing_table_name', 'schema_json'])
            ->caller(__METHOD__)
            ->fetchResultSet();

		$schemas = [];
		$backingBucketName = [];
		foreach ( $res as $row ) {
			$schemas[$row->table_name] = json_decode( $row->schema_json, true );
			$backingBucketName[$row->table_name] = $row->backing_table_name;
		}

        $output = [];

        $output[] = "<table class=\"wikitable\"><tr>";
        $output[] = "<th>Bucket</th><th>Redirect target</th>";
        foreach( $schemas as $row => $val ) {
            $output[] = "<tr>";
            $output[] = "<td>[[Bucket:$row]]</td>";
            if ( $backingBucketName[$row] != null ) {
                $output[] = "<td>[[Bucket:$backingBucketName[$row]]]</td>";
            } else {
                $output[] = "<td></td>";
            }
            $output[] = "</tr>";
        }
        $output[] = "</table>";

        $out->addWikiTextAsContent(implode('', $output));

    }

    function getGroupName() {
        return 'bucket';
    }
}