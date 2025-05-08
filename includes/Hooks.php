<?php

namespace MediaWiki\Extension\Bucket;

use MediaWiki\Content\JsonContent;
use MediaWiki\Extension\Bucket\BucketPage;
use MediaWiki\Extension\Scribunto\Hooks\ScribuntoExternalLibrariesHook;
use MediaWiki\Hook\LinksUpdateCompleteHook;
use MediaWiki\Hook\SkinBuildSidebarHook;
use MediaWiki\Installer\Hook\LoadExtensionSchemaUpdatesHook;
use MediaWiki\Page\Hook\ArticleFromTitleHook;
use MediaWiki\Storage\Hook\MultiContentSaveHook;
use MediaWiki\Revision\SlotRecord;
use MediaWiki\Title\Title;
use MediaWiki\Page\Article;
use MediaWiki\Context\IContextSource;

class Hooks implements
	LinksUpdateCompleteHook,
	LoadExtensionSchemaUpdatesHook,
	MultiContentSaveHook,
	ScribuntoExternalLibrariesHook,
	SkinBuildSidebarHook,
	ArticleFromTitleHook
{
	/**
	 * @see https://www.mediawiki.org/wiki/Manual:Hooks/LoadExtensionSchemaUpdates
	 *
	 * @param DatabaseUpdater $updater
	 * @return bool|void
	 */
	public function onLoadExtensionSchemaUpdates( $updater ) {
		$updater->addExtensionTable( 'bucket_schemas', __DIR__ . '/../sql/create_bucket_schemas.sql' );
		$updater->addExtensionTable( 'bucket_pages', __DIR__ . '/../sql/create_bucket_pages.sql' );
	}

	/**
	 * @see https://www.mediawiki.org/wiki/Manual:Hooks/LinksUpdateComplete
	 *
	 * @param LinksUpdate $linksUpdate
	 * @param mixed $ticket
	 * @return bool|void
	 */
	public function onLinksUpdateComplete( $linksUpdate, $ticket ) {
		$bucketPuts = $linksUpdate->getParserOutput()->getExtensionData( Bucket::EXTENSION_DATA_KEY );
		$pageId = $linksUpdate->getTitle()->getArticleID();
		if ( $bucketPuts !== null ) {
			// file_put_contents(MW_INSTALL_PATH . '/cook.txt', "HOOK " . print_r($bucketPuts, true) . "\n", FILE_APPEND);
			$titleText = $linksUpdate->getTitle()->getPrefixedText();
			Bucket::writePuts($pageId, $titleText, $bucketPuts);
		} else {
			Bucket::clearOrphanedData($pageId);
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
			//TODO why are we getting into here?
			// return;
		}
		$jsonSchema = $content->getData()->value;
		$title = $page->getDBkey();
		Bucket::createOrModifyTable( $title, $jsonSchema, $revRecord->getParentId() );
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

	/**
	 * @see https://www.mediawiki.org/wiki/Manual:Hooks/SidebarBeforeOutput
	 * 
	 * @param Skin $skin
	 * @param array $bar
	 * @return void
	 */
	public function onSkinBuildSidebar( $skin, &$bar ) {
		//TODO check namespace
		//TODO this should be TOOLBOX but that makes the entry not show up
		$bar['toolbox'][] = [
			'text' => 'View Bucket',
			'href' => '?action=bucket',
			'title' => 'Bucket',
			'id' => 'n-bucket'
		];
	}

	/**
	 * @see https://www.mediawiki.org/wiki/Manual:Hooks/ArticleFromTitle
	 * 
	 * @param Title $title
	 * @param Article|null $article
	 * @param IContextSource $context
	 */
	public function onArticleFromTitle( $title, &$article, $context) {
		if ( $title->getNamespace() !== NS_BUCKET ) {
			return;
		}
		$out = $context->getOutput();
		$article = new BucketPage( $title );
	}
}
