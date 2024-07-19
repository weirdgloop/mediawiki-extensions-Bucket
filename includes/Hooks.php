<?php

namespace MediaWiki\Extension\Bucket;

use MediaWiki\Content\JsonContent;
use MediaWiki\Extension\Scribunto\Hooks\ScribuntoExternalLibrariesHook;
use MediaWiki\Hook\LinksUpdateCompleteHook;
use MediaWiki\Hook\LoadExtensionSchemaUpdatesHook;
use MediaWiki\Hook\MultiContentSaveHook;
use MediaWiki\Revision\SlotRecord;

class Hooks implements
	LinksUpdateCompleteHook,
	LoadExtensionSchemaUpdatesHook,
	MultiContentSaveHook,
	ScribuntoExternalLibrariesHook
{
	/**
	 * @see https://www.mediawiki.org/wiki/Manual:Hooks/LoadExtensionSchemaUpdates
	 *
	 * @param DatabaseUpdater $updater
	 * @return bool|void
	 */
	public function onLoadExtensionSchemaUpdates( $updater ) {
		$updater->addExtensionTable( 'bucket__drops', __DIR__ . '/../sql/create_tables.sql' );
	}

	/**
	 * @see https://www.mediawiki.org/wiki/Manual:Hooks/LinksUpdateComplete
	 *
	 * @param LinksUpdate $linksUpdate
	 * @param mixed $ticket
	 * @return bool|void
	 */
	public function onLinksUpdateComplete( $linksUpdate, $ticket ) {
		$bucketPuts = $linksUpdate->getParserOutput()->bucketPuts;
		if ( isset( $bucketPuts ) ) {
			$pageId = $linksUpdate->getTitle()->getArticleID();
			$titleText = $linksUpdate->getParserOutput()->getTitleText();
			Bucket::writePuts( $pageId, $titleText, $bucketPuts );
		}
	}

	/**
	 * @see https://www.mediawiki.org/wiki/Manual:Hooks/MultiContentSave
	 *
	 * @param RenderedRevision $renderedRevision
	 * @param UserIdentity $user
	 * @param CommentStoreComment $summary
	 * @param int $flags
	 * @param Status $status
	 * @return bool|void
	 */
	public function onMultiContentSave( $renderedRevision, $user, $summary, $flags, $status ) {
		$revRecord = $renderedRevision->getRevision();
		$page = $revRecord->getPage();
		if ( $page->getNamespace() !== NS_BUCKET ) {
			return;
		}
		$content = $revRecord->getContent( SlotRecord::MAIN );
		if ( !$content instanceof JsonContent || !$content->isValid() ) {
			// This will fail anyway before saving.
			return;
		}
		$jsonSchema = $content->getData()->value;
		$title = $page->getDBkey();
		Bucket::createOrModifyTable( $title, $jsonSchema );
	}

	/**
	 * @see https://www.mediawiki.org/wiki/Manual:Hooks/ScribuntoExternalLibraries
	 *
	 * @param string $engine
	 * @param array &$extraLibraries
	 * @return bool|void
	 */
	public function onScribuntoExternalLibraries( $engine, &$extraLibraries ) {
		if ( $engine === 'lua' ) {
			$extraLibraries['mw.ext.bucket'] = LuaLibrary::class;
		}
	}
}
