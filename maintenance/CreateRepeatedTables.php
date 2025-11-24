<?php

namespace MediaWiki\Extension\Bucket;

use MediaWiki\Maintenance\Maintenance;
use MediaWiki\MediaWikiServices;
use Wikimedia\Rdbms\DBQueryError;

$IP = getenv( 'MW_INSTALL_PATH' );
if ( $IP === false ) {
	$IP = __DIR__ . '/../../..';
}
require_once "$IP/maintenance/Maintenance.php";
require_once "$IP/extensions/Bucket/includes/Bucket.php";

/**
 * Create a table for each repeated field in a bucket
 *
 * Migration guide to new repeated fields:
 * 1) Run this script
 * 2) Reparse all bucket writing pages
 * 3) $wgBucketForceOldRepeatedQuery = false
 * 4) remove the index from JSON type columns (to avoid issues with size limits)
 */
class CreateRepeatedTables extends Maintenance {
	public function __construct() {
		parent::__construct();
		$this->requireExtension( 'Bucket' );
		$this->addDescription( 'Create a table for each repeated field in a bucket' );
		$this->addOption( 'dry-run', 'Only print the commands without executing them.', false, false );
	}

	public function execute() {
		$config = MediaWikiServices::getInstance()->getMainConfig();
		$bucketDBuser = $this->getOption( 'bucket_user', $config->get( 'BucketDBuser' ) );
		$bucketDBhostname = $this->getOption( 'bucket_hostname', $config->get( 'BucketDBhostname' ) );

		if ( $bucketDBuser === null ) {
			$this->output( "Cannot find a Bucket username.\nEither pass --bucket_user or set \$wgBucketDBuser.\n" );
			return false;
		}
		$dbw = $this->getDB( DB_PRIMARY );
		$dbName = $dbw->getDBname();
		$fullUserName = "$bucketDBuser@'$bucketDBhostname'";

		$query = [];

		// Grab existing Bucket tables
		$res = $dbw->newSelectQueryBuilder()
			->from( 'bucket_schemas' )
			->select( [ 'bucket_name', 'schema_json' ] )
			->forUpdate()
			->caller( __METHOD__ )
			->fetchResultSet();
		foreach ( $res as $row ) {
			$bucketSchema = new BucketSchema(
				$row->bucket_name,
				json_decode( $row->schema_json, true )
			);

			$tableNames = [];
			foreach ( $bucketSchema->getFields() as $field ) {
				if ( $field->getRepeated() ) {
					$statement = BucketDatabase::getCreateRepeatedTableStatement( $bucketSchema, $field, $dbw );
					$tableNames[] = $statement[0];
					$query[] = $statement[1];
				}
			}

			foreach ( $tableNames as $tableName ) {
				$table = $dbw->tableName( $tableName );
				$query[] =
					"GRANT SELECT, INSERT, UPDATE, DELETE, CREATE, ALTER, DROP ON `$dbName`.$table TO $fullUserName;";
			}
		}

		foreach ( $query as $singleQuery ) {
			$this->output( $singleQuery . "\n" );
			if ( !$this->getOption( 'dry-run' ) ) {
				try {
					$dbw->query( $singleQuery );
				} catch ( DBQueryError $e ) {
					if ( str_contains( $e->getMessage(), 'denied to user' ) ) {
						$this->output(
							"The database user \"{$this->getConfig()->get( 'DBuser' )}\" needs permission to "
							. 'to execute the required queries to set up Bucket. The user needs to be able to GRANT '
							. "privileges to another database user ($fullUserName).\n"
						);
						break;
					} else {
						throw $e;
					}
				}
			}
		}

		if ( $this->getOption( 'dry-run' ) ) {
			$this->output( "--dry-run option present. No changes have been made.\n" );
		}
		return true;
	}
}

$maintClass = CreateRepeatedTables::class;
require_once RUN_MAINTENANCE_IF_MAIN;
