<?php

namespace MediaWiki\Extension\Bucket;

use JsonContent;
use ManualLogEntry;
use MediaWiki\Content\Hook\ContentModelCanBeUsedOnHook;
use MediaWiki\Context\IContextSource;
use MediaWiki\Deferred\LinksUpdate\LinksUpdate;
use MediaWiki\Extension\Scribunto\Hooks\ScribuntoExternalLibrariesHook;
use MediaWiki\Hook\LinksUpdateCompleteHook;
use MediaWiki\Hook\MovePageIsValidMoveHook;
use MediaWiki\Hook\SidebarBeforeOutputHook;
use MediaWiki\Hook\TitleIsAlwaysKnownHook;
use MediaWiki\Hook\TitleIsMovableHook;
use MediaWiki\Installer\Hook\LoadExtensionSchemaUpdatesHook;
use MediaWiki\MediaWikiServices;
use MediaWiki\Page\Article;
use MediaWiki\Page\Hook\ArticleFromTitleHook;
use MediaWiki\Page\Hook\BeforeDisplayNoArticleTextHook;
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
	SidebarBeforeOutputHook,
	ArticleFromTitleHook,
	MovePageIsValidMoveHook,
	PageDeleteHook,
	PageDeleteCompleteHook,
	ContentModelCanBeUsedOnHook,
	BeforeDisplayNoArticleTextHook,
	TitleIsAlwaysKnownHook,
	TitleIsMovableHook
{
	private function enabledNamespaces() {
		$config = MediaWikiServices::getInstance()->getMainConfig();
		return array_keys( $config->get( 'BucketWriteEnabledNamespaces' ), true );
	}

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
		if ( $linksUpdate->getTitle()->inNamespaces( $this->enabledNamespaces() ) ) {
			if ( $bucketPuts !== null ) {
				$titleText = $linksUpdate->getTitle()->getPrefixedText();
				$bucket = new Bucket();
				$bucket->writePuts( $pageId, $titleText, $bucketPuts );
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
		} catch ( BucketException $e ) {
			$status->fatal( $e->getWfMessage() );
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
		$isExistingPage = $revRecord->getParentId() > 0;
		Bucket::createOrModifyTable( $title, $jsonSchema, $isExistingPage );
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
		$isExistingPage = $revRecord->getParentId() > 0;
		try {
			Bucket::createOrModifyTable( $title, $jsonSchema, $isExistingPage );
		} catch ( BucketException $e ) {
			$status->fatal( $e->getWfMessage() );
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
	 */
	public function onSidebarBeforeOutput( $skin, &$sidebar ): void {
		if ( $skin->getTitle()->inNamespaces( $this->enabledNamespaces() ) ) {
			$sidebar['TOOLBOX'][] = [
				'text' => $skin->msg( 'bucket-sidebar-action' )->text(),
				'href' => $skin->getTitle()->getLocalURL( 'action=bucket' ),
				'title' => 'Bucket',
				'id' => 'n-bucket'
			];
		}
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
	 * @see https://www.mediawiki.org/wiki/Manual:Hooks/TitleIsMovable
	 */
	public function onTitleIsMovable( $title, &$result ) {
		if ( $title->getNamespace() !== NS_BUCKET ) {
			return;
		}

		$result = false;
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

		try {
			$pagesCount = Bucket::countPagesUsingBucket( $page->getDBkey() );
			if ( $pagesCount == 0 ) {
				return true;
			} else {
				$status->fatal( 'bucket-delete-fail-in-use', $pagesCount );
				return false;
			}
		// If we somehow get a page that isn't a valid Bucket name, it will throw a schema exception.
		} catch ( SchemaException $e ) {
			$status->warning( $e->getWfMessage() );
			return true;
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
		try {
			Bucket::deleteTable( $page->getDBkey() );
		// If we somehow get a page that isn't a valid Bucket name, it will throw a schema exception.
		} catch ( SchemaException $e ) {
			return;
		}
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

	/**
	 * @see https://www.mediawiki.org/wiki/Manual:Hooks/BeforeDisplayNoArticleText
	 */
	public function onBeforeDisplayNoArticleText( $article ) {
		if ( $article->getTitle()->getNamespace() !== NS_BUCKET ) {
			return;
		}

		if ( strtolower( str_replace( ' ', '_', $article->getTitle()->getRootText() ) ) == Bucket::MESSAGE_BUCKET ) {
			return false;
		}
	}

	/**
	 * @see https://www.mediawiki.org/wiki/Manual:Hooks/TitleIsAlwaysKnown
	 */
	public function onTitleIsAlwaysKnown( $title, &$isKnown ) {
		if ( $title->getNamespace() !== NS_BUCKET ) {
			return;
		}

		if ( strtolower( str_replace( ' ', '_', $title->getRootText() ) ) == Bucket::MESSAGE_BUCKET ) {
			$isKnown = true;
		}
	}
}
