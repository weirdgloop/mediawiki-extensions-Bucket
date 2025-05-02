<?php

namespace MediaWiki\Extension\Bucket;

use MediaWiki\SpecialPage\SpecialPage;

class BucketSpecial extends SpecialPage {
    public function __construct() {
        parent::__construct( 'bucket' );
    }

    public function execute( $par ) {
        $request = $this->getRequest();
		$output = $this->getOutput();
		$this->setHeaders();

		# Get request data from, e.g.
		$param = $request->getText( 'param' );

		# Do stuff
		# ...
		$wikitext = 'Hello world!';
		$output->addWikiTextAsInterface( $wikitext );
    }

    function getGroupName() {
        return 'bucket';
    }
}