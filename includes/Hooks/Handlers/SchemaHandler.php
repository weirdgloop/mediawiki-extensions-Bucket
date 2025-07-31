<?php

namespace MediaWiki\Extension\Bucket\Hooks\Handlers;

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

		$dbType = $updater->getDB()->getType();
		if ( $dbType !== 'mysql' ) {
			$updater->output( "Bucket only supports MySQL. Skipping schema updates for $dbType.\n" );
			return;
		}

		$updater->addExtensionTable( 'bucket_pages', "$dir/tables-generated.sql" );
		$updater->addExtensionUpdate( [ [ $this, 'createInitialSchemaForBucketMessage' ] ] );
	}

	/**
	 * @param DatabaseUpdater $updater
	 * @return bool
	 */
	public function createInitialSchemaForBucketMessage( DatabaseUpdater $updater ) {
		$schema = [
			'_page_id' => [ 'type' => 'INTEGER', 'index' => false, 'repeated' => false ],
			'_index' => [ 'type' => 'INTEGER', 'index' => false, 'repeated' => false ],
			'page_name' => [ 'type' => 'PAGE', 'index' => true, 'repeated' => false ],
			'page_name_sub' => [ 'type' => 'PAGE', 'index' => true, 'repeated' => false ],
			'bucket' => [ 'type' => 'PAGE', 'index' => true, 'repeated' => false ],
			'message' => [ 'type' => 'TEXT', 'index' => true, 'repeated' => false ],
			'property' => [ 'type' => 'TEXT', 'index' => true, 'repeated' => false ],
			'type' => [ 'type' => 'TEXT', 'index' => true, 'repeated' => false ]
		];

		if ( !$updater->updateRowExists( 'bucket-message-schema' ) ) {
			$db = $updater->getDB();
			$db->newInsertQueryBuilder()
				->table( 'bucket_schemas' )
				->rows( [ [
					'bucket_name' => 'bucket_message',
					'schema_json' => json_encode( $schema )
				] ] )
				->caller( __METHOD__ )
				->execute();

			if ( $db->affectedRows() > 0 ) {
				$updater->insertUpdateRow( 'bucket-message-schema' );
				return true;
			}
		}

		return false;
	}
}
