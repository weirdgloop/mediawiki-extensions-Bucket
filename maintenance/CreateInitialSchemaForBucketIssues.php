<?php

namespace MediaWiki\Extension\Bucket;

use MediaWiki\Maintenance\LoggedUpdateMaintenance;

/**
 * Creates the initial schema for the bucket_issues bucket.
 */
class CreateInitialSchemaForBucketIssues extends LoggedUpdateMaintenance {

	public function __construct() {
		parent::__construct();
		$this->requireExtension( 'Bucket' );
		$this->addDescription( 'Creates the initial schema for the bucket_issues bucket.' );
	}

	/** @inheritDoc */
	protected function doDBUpdates(): bool {
		$dbw = $this->getPrimaryDB();

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

		$dbw->newInsertQueryBuilder()
			->table( 'bucket_schemas' )
			->rows( [ [
				'bucket_name' => Bucket::ISSUES_BUCKET,
				'schema_json' => json_encode( $schema )
			] ] )
			->caller( __METHOD__ )
			->execute();

		if ( $dbw->affectedRows() > 0 ) {
			$this->output( "Created the initial schema for the bucket_issues bucket.\n" );
			return true;
		}

		$this->output( "Failed to create the inital schema for the bucket_issues bucket.\n" );
		return false;
	}

	/** @inheritDoc */
	protected function getUpdateKey(): string {
		return 'bucket-issues-schema';
	}

}

$maintClass = CreateInitialSchemaForBucketIssues::class;
require_once RUN_MAINTENANCE_IF_MAIN;
