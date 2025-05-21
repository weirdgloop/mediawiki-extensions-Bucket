<?php

namespace MediaWiki\Extension\Bucket;

use Exception;
use JsonContent;
use ManualLogEntry;
use MediaWiki\Content\Hook\ContentModelCanBeUsedOnHook;
use MediaWiki\Context\IContextSource;
use MediaWiki\Deferred\LinksUpdate\LinksUpdate;
use MediaWiki\Extension\Scribunto\Hooks\ScribuntoExternalLibrariesHook;
use MediaWiki\Hook\LinksUpdateCompleteHook;
use MediaWiki\Hook\MovePageIsValidMoveHook;
use MediaWiki\Hook\SkinBuildSidebarHook;
use MediaWiki\Installer\Hook\LoadExtensionSchemaUpdatesHook;
use MediaWiki\MediaWikiServices;
use MediaWiki\Page\Article;
use MediaWiki\Page\Hook\ArticleFromTitleHook;
use MediaWiki\Page\Hook\PageDeleteCompleteHook;
use MediaWiki\Page\Hook\PageDeleteHook;
use MediaWiki\Page\Hook\PageUndeleteCompleteHook;
use MediaWiki\Page\Hook\PageUndeleteHook;
use MediaWiki\Page\ProperPageIdentity;
use MediaWiki\Permissions\Authority;
use MediaWiki\Revision\RenderedRevision;
use MediaWiki\Revision\RevisionRecord;
use MediaWiki\Revision\SlotRecord;
use MediaWiki\Status\Status;
use MediaWiki\Storage\Hook\MultiContentSaveHook;
use MediaWiki\Title\Title;
use StatusValue;

class Hooks implements
	LinksUpdateCompleteHook,
	LoadExtensionSchemaUpdatesHook,
	MultiContentSaveHook,
	PageUndeleteHook,
	PageUndeleteCompleteHook,
	ScribuntoExternalLibrariesHook,
	SkinBuildSidebarHook,
	ArticleFromTitleHook,
	MovePageIsValidMoveHook,
	PageDeleteHook,
	PageDeleteCompleteHook,
	ContentModelCanBeUsedOnHook
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
		$config = MediaWikiServices::getInstance()->getMainConfig();
		if ( $linksUpdate->getTitle()->inNamespaces( array_keys( $config->get( 'BucketWriteEnabledNamespaces' ) ) ) ) {
			if ( $bucketPuts !== null ) {
				$titleText = $linksUpdate->getTitle()->getPrefixedText();
				Bucket::writePuts( $pageId, $titleText, $bucketPuts );
			} else {
				Bucket::clearOrphanedData( $pageId );
			}
		}
	}

	/**
	 * @see https://www.mediawiki.org/wiki/Manual:Hooks/PageUndelete
	 */
	public function onPageUndelete( ProperPageIdentity $page, Authority $performer, string $reason, bool $unsuppress, array $timestamps, array $fileVersions, StatusValue $status ) {
		if ( $page->getNamespace() !== NS_BUCKET ) {
			return;
		}
		$title = $page->getDBkey();
		try {
			if ( Bucket::canCreateTable( $title ) ) {
				return true;
			} else {
				$status->fatal( 'bucket-undelete-error' );
				return false;
			}
		} catch ( Exception $e ) {
			$status->fatal( $e->getMessage() );
			return false;
		}
	}

	/**
	 * @see https://www.mediawiki.org/wiki/Manual:Hooks/PageUndeleteComplete
	 */
	public function onPageUndeleteComplete( ProperPageIdentity $page, Authority $restorer, string $reason, RevisionRecord $restoredRev, ManualLogEntry $logEntry, int $restoredRevisionCount, bool $created, array $restoredPageIds ): void {
		$revRecord = $restoredRev;
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
		$parentId = $revRecord->getParentId() ?? 0;
		Bucket::createOrModifyTable( $title, $jsonSchema, $parentId );
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
		$parentId = $revRecord->getParentId() ?? 0;
		try {
			Bucket::createOrModifyTable( $title, $jsonSchema, $parentId );
		} catch ( Exception $e ) {
			$status->fatal( $e->getMessage() );
			return false;
		}
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
	 * @param array &$bar
	 * @return void
	 */
	public function onSkinBuildSidebar( $skin, &$bar ) {
		// TODO check namespace
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
	 * @param Article|null &$article
	 * @param IContextSource $context
	 */
	public function onArticleFromTitle( $title, &$article, $context ) {
		if ( $title->getNamespace() !== NS_BUCKET ) {
			return;
		}
		$article = new BucketPage( $title );
	}

	/**
	 * @see https://www.mediawiki.org/wiki/Manual:Hooks/MovePageIsValidMove
	 */
	public function onMovePageIsValidMove( $oldTitle, $newTitle, $status ) {
		if ( $oldTitle->getNamespace() !== NS_BUCKET && $newTitle->getNamespace() !== NS_BUCKET ) {
			return;
		}

		if ( $oldTitle->getNamespace() !== NS_BUCKET ) {
			$status->fatal( 'bucket-namespace-move-into' );
		} else {
			$status->fatal( 'bucket-namespace-move' );
		}
	}

	/**
	 * $see https://www.mediawiki.org/wiki/Manual:Hooks/PageDelete
	 */
	public function onPageDelete( ProperPageIdentity $page, Authority $deleter, string $reason, StatusValue $status, bool $suppress ) {
		if ( $page->getNamespace() !== NS_BUCKET ) {
			return true;
		}

		if ( Bucket::canDeleteBucketPage( $page->getDBkey() ) ) {
			return true;
		} else {
			$status->fatal( 'bucket-delete-fail-in-use' );
			return false;
		}
	}

	/**
	 * @see https://www.mediawiki.org/wiki/Manual:Hooks/PageDeleteComplete
	 */
	public function onPageDeleteComplete( ProperPageIdentity $page, Authority $deleter, string $reason, int $pageID, RevisionRecord $deletedRev, ManualLogEntry $logEntry, int $archivedRevisionCount ) {
		if ( $page->getNamespace() !== NS_BUCKET ) {
			Bucket::clearOrphanedData( $page->getId() );
			return;
		}
		Bucket::deleteTable( $page->getDBkey() );
	}

	/**
	 * @see https://www.mediawiki.org/wiki/Manual:Hooks/ContentModelCanBeUsedOn
	 */
	public function onContentModelCanBeUsedOn( $contentModel, $title, &$ok ) {
		if ( $title->getNamespace() !== NS_BUCKET ) {
			return;
		} elseif ( $contentModel != 'json' ) {
			$ok = false;
			return false;
		}
	}
}
