<?php

namespace MediaWiki\Extension\Bucket;

use MediaWiki\MediaWikiServices;

return [
	'Bucket.BucketDatabase' => static function ( MediaWikiServices $services ): BucketDatabase {
		return new BucketDatabase(
			$services->getMainConfig(),
			$services->getDBLoadBalancer(),
			$services->getDatabaseFactory()
		);
	}
];
