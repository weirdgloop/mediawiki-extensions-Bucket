<?php

namespace MediaWiki\Extension\Bucket;

use MediaWiki\Maintenance\Maintenance;
use MediaWiki\MediaWikiServices;

$IP = getenv( 'MW_INSTALL_PATH' );
if ( $IP === false ) {
	$IP = __DIR__ . '/../../..';
}
require_once "$IP/maintenance/Maintenance.php";

/**
 * Sets up required permissions for a Bucket user
 */
class SetupDBPermission extends Maintenance {
	public function __construct() {
		parent::__construct();
		$this->requireExtension( 'Bucket' );
		$this->addDescription( 'Sets up the required permissions for a Bucket database user.' );
		$this->addOption( 'bucket_user', 'The database user name to grant permissions to. Defaults to $wgBucketDBuser.', false, true );
		$this->addOption( 'bucket_hostname', 'The database hostname. Defaults to $wgBucketDBhostname.', false, true );
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
		$fullUserName = "$bucketDBuser@'$bucketDBhostname'";

		// Check if user actually exists
		$res = $dbw->query( "SELECT EXISTS(SELECT 1 from mysql.user WHERE user = '$bucketDBuser' AND host = '$bucketDBhostname')" );
		if ( $res->fetchRow()[0] == 1 ) {
			print( "User $fullUserName exists.\n" );
		} else {
			print( "User $fullUserName does not exist in the database.\nCheck for typos, or create the specified user and then run this script again.\n" );
			return false;
		}

		$query = [];

		// We only need to be able to select from the categorylinks table.
		$query[] = "GRANT SELECT ON `categorylinks` TO $fullUserName;";

		// These tables we want to select, insert, and delete rows.
		$specialTables = [ 'bucket_pages', 'bucket_schemas' ];
		foreach ( $specialTables as $table ) {
			$query[] = "GRANT SELECT, INSERT, UPDATE, DELETE ON `$table` TO $fullUserName;";
		}

		// Grab existing Bucket tables, in case we are migrating an existing install to a new account.
		$res = $dbw->newSelectQueryBuilder()
					->from( 'bucket_schemas' )
					->select( [ 'bucket_name' ] )
					->forUpdate()
					->caller( __METHOD__ )
					->fetchResultSet();
		foreach ( $res as $row ) {
			$table = $dbw->addIdentifierQuotes( 'bucket__' . $row->bucket_name );
			$query[] = "GRANT SELECT, INSERT, UPDATE, DELETE, CREATE, ALTER, DROP ON $table TO $fullUserName;";
		}

		foreach ( $query as $singleQuery ) {
			print( $singleQuery . "\n" );
			if ( !$this->getOption( 'dry-run' ) ) {
				$dbw->query( $singleQuery );
			}
		}

		if ( $this->getOption( 'dry-run' ) ) {
			print( "--dry-run option present. No changes have been made.\n" );
		}
		return true;
	}
}

$maintClass = SetupDBPermission::class;
require_once RUN_MAINTENANCE_IF_MAIN;
