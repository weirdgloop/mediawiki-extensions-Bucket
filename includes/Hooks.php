<?php

namespace MediaWiki\Extension\Bucket;

use MediaWiki\MediaWikiServices;
use MediaWiki\Revision\RenderedRevision;
use MediaWiki\Revision\SlotRecord;
use MediaWiki\User\UserIdentity;
class Hooks {

	public static function registerExtension() {
		define( 'BUCKET_VERSION', '0.1' );
	}

	public static function initialize() {

	}

	public static function log($val) {
		$dbw = wfGetDB( DB_MASTER );
		$dbw->insert("logs", ["text" => $val]);
	}

	public static function createTables( DatabaseUpdater $updater ) {
		$updater->addExtensionTable( 'bucket__drops', __DIR__ . "/sql/create_tables.sql" );
		return true;
	}

	public static function addLuaLibrary( $engine, &$extraLibraries ) {
		$extraLibraries['mw.ext.bucket'] = 'BucketLuaLibrary';
		return true;
	}

	public static function onLinksUpdateComplete( &$linksUpdate ) {
		$bucketPuts = $linksUpdate->getParserOutput()->bucketPuts;
		if (isset($bucketPuts)) {
			$pageId = $linksUpdate->getTitle()->getArticleID();
			$titleText = $linksUpdate->getParserOutput()->getTitleText();
			Bucket::writePuts($pageId, $titleText, $bucketPuts);
		}
	}

	public static function onMultiContentSave( RenderedRevision $renderedRevision, UserIdentity $user, CommentStoreComment $summary, $flags, Status $hookStatus ) {
		$ns = $renderedRevision->getRevision()->getPage()->getNamespace();
		if ($ns !== NS_BUCKET) {
			return;
		}
		$jsonContent = $renderedRevision->getRevision()->getContent("main");
		if (!$jsonContent->isValid()) {
			// TODO: check if different type.
			// This will fail anyway before saving.
			return;
		}
		$jsonSchema = $jsonContent->getData()->value;
		$title = $renderedRevision->getRevision()->getPage()->getDBkey();
		self::log($title);
		// self::log(get_class($jsonSchema));
		Bucket::createOrModifyTable($title, $jsonSchema);
	}
}
