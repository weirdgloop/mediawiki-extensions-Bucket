<?php

namespace MediaWiki\Extension\Bucket\Hooks\Handlers;

use MediaWiki\Extension\Bucket\CreateInitialSchemaForBucketIssues;
use MediaWiki\Installer\DatabaseUpdater;
use MediaWiki\Installer\Hook\LoadExtensionSchemaUpdatesHook;

class SchemaHandler implements LoadExtensionSchemaUpdatesHook {
	/**
	 * @see https://www.mediawiki.org/wiki/Manual:Hooks/LoadExtensionSchemaUpdates
	 *
	 * @param DatabaseUpdater $updater
	 * @return void
	 */
	public function onLoadExtensionSchemaUpdates( $updater ) {
		$dir = dirname( __DIR__, 3 ) . '/sql';

		$updater->addExtensionTable( 'bucket_pages', "$dir/tables-generated.sql" );
		$updater->addExtensionTable( 'bucket__bucket_issues', "$dir/issues-table.sql" );
		$updater->addPostDatabaseUpdateMaintenance( CreateInitialSchemaForBucketIssues::class );
	}
}
