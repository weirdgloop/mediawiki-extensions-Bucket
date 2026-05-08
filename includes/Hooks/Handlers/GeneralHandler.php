<?php

namespace MediaWiki\Extension\Bucket\Hooks\Handlers;

use ManualLogEntry;
use MediaWiki\Config\Config;
use MediaWiki\Content\Hook\ContentModelCanBeUsedOnHook;
use MediaWiki\Content\JsonContent;
use MediaWiki\Extension\Bucket\Bucket;
use MediaWiki\Extension\Bucket\BucketDatabase;
use MediaWiki\Extension\Bucket\BucketException;
use MediaWiki\Extension\Bucket\BucketPage;
use MediaWiki\Extension\Bucket\BucketQuery;
use MediaWiki\Extension\Bucket\LuaLibrary;
use MediaWiki\Extension\Scribunto\Hooks\ScribuntoExternalLibrariesHook;
use MediaWiki\Hook\AfterImportPageHook;
use MediaWiki\Hook\LinksUpdateCompleteHook;
use MediaWiki\Hook\PageMoveCompleteHook;
use MediaWiki\Hook\ParserClearStateHook;
use MediaWiki\Hook\ParserLimitReportPrepareHook;
use MediaWiki\Hook\SidebarBeforeOutputHook;
use MediaWiki\Hook\TitleIsAlwaysKnownHook;
use MediaWiki\Page\Hook\ArticleFromTitleHook;
use MediaWiki\Page\Hook\BeforeDisplayNoArticleTextHook;
use MediaWiki\Page\Hook\PageDeleteCompleteHook;
use MediaWiki\Page\Hook\PageDeleteHook;
use MediaWiki\Page\Hook\PageUndeleteCompleteHook;
use MediaWiki\Page\Hook\PageUndeleteHook;
use MediaWiki\Page\ProperPageIdentity;
use MediaWiki\Permissions\Authority;
use MediaWiki\Revision\RevisionLookup;
use MediaWiki\Revision\RevisionRecord;
use MediaWiki\Revision\SlotRecord;
use MediaWiki\Storage\Hook\MultiContentSaveHook;
use StatusValue;

class GeneralHandler implements
	LinksUpdateCompleteHook,
	MultiContentSaveHook,
	PageUndeleteHook,
	PageUndeleteCompleteHook,
	ScribuntoExternalLibrariesHook,
	SidebarBeforeOutputHook,
	ArticleFromTitleHook,
	PageDeleteHook,
	PageDeleteCompleteHook,
	ContentModelCanBeUsedOnHook,
	BeforeDisplayNoArticleTextHook,
	TitleIsAlwaysKnownHook,
	AfterImportPageHook,
	ParserClearStateHook,
	ParserLimitReportPrepareHook,
	PageMoveCompleteHook
{

	public function __construct(
		private readonly Config $config,
		private readonly RevisionLookup $revisionLookup,
	) {
	}

	private function getEnabledNamespaces(): array {
		return array_keys( $this->config->get( 'BucketWriteEnabledNamespaces' ), true );
	}

	/** @inheritDoc */
	public function onLinksUpdateComplete( $linksUpdate, $ticket ): void {
		if ( $linksUpdate->getTitle()->inNamespaces( $this->getEnabledNamespaces() ) ) {
			$bucketPutsKeys = $linksUpdate->getParserOutput()->getExtensionData( Bucket::EXTENSION_DATA_KEY ) ?? [];
			$pageId = $linksUpdate->getTitle()->getArticleID();
			$titleText = $linksUpdate->getTitle()->getPrefixedText();

			$bucketPuts = [];
			foreach ( array_keys( $bucketPutsKeys ) as $key ) {
				$singlePut = $linksUpdate->getParserOutput()->getExtensionData( Bucket::EXTENSION_DATA_KEY . $key );
				$bucketName = $singlePut['bucket'];
				if ( !isset( $bucketPuts[$bucketName] ) ) {
					$bucketPuts[$bucketName] = [];
				}
				$bucketPuts[$bucketName][] = $singlePut;
			}
			Bucket::writePuts( $pageId, $titleText, $bucketPuts );
		}
	}

	/** @inheritDoc */
	public function onPageUndelete(
		ProperPageIdentity $page, Authority $performer, string $reason, bool $unsuppress, array $timestamps,
		array $fileVersions, StatusValue $status
	): bool {
		if ( $page->getNamespace() !== NS_BUCKET ) {
			return true;
		}
		$title = $page->getDBkey();
		try {
			if ( BucketDatabase::canCreateTable( $title ) ) {
				return true;
			} else {
				$status->fatal( 'bucket-undelete-error' );
				return false;
			}
		} catch ( BucketException $e ) {
			$status->fatal( $e->getwfMessage() );
			return false;
		}
	}

	/** @inheritDoc */
	public function onPageUndeleteComplete(
		ProperPageIdentity $page, Authority $restorer, string $reason, RevisionRecord $restoredRev,
		ManualLogEntry $logEntry, int $restoredRevisionCount, bool $created, array $restoredPageIds
	): void {
		$page = $restoredRev->getPage();
		if ( $page->getNamespace() !== NS_BUCKET ) {
			return;
		}
		$content = $restoredRev->getContent( SlotRecord::MAIN );
		if ( !$content instanceof JsonContent || !$content->isValid() ) {
			// This will fail anyway before saving.
			return;
		}
		$jsonSchema = $content->getData()->value;
		$title = $page->getDBkey();
		$isExistingPage = $restoredRev->getParentId() !== null;
		BucketDatabase::createOrModifyTable( $title, $jsonSchema, $isExistingPage );
	}

	/** @inheritDoc */
	public function onMultiContentSave( $renderedRevision, $user, $summary, $flags, $status ): bool {
		$revRecord = $renderedRevision->getRevision();
		$page = $revRecord->getPage();
		if ( $page->getNamespace() !== NS_BUCKET ) {
			return true;
		}
		$content = $revRecord->getContent( SlotRecord::MAIN );
		if ( !$content instanceof JsonContent || !$content->isValid() ) {
			// This will fail anyway before saving.
			return true;
		}
		$jsonSchema = $content->getData()->value;
		$title = $page->getDBkey();
		$isExistingPage = $revRecord->getParentId() !== null;
		try {
			BucketDatabase::createOrModifyTable( $title, $jsonSchema, $isExistingPage );
		} catch ( BucketException $e ) {
			$status->fatal( $e->getwfMessage() );
			return false;
		}
	}

	/** @inheritDoc */
	public function onAfterImportPage( $title, $foreignTitle, $revCount, $sRevCount, $pageInfo ): void {
		if ( $title->getNamespace() !== NS_BUCKET ) {
			return;
		}
		$content = $this->revisionLookup->getRevisionByTitle( $title )->getContent( SlotRecord::MAIN );
		if ( !$content instanceof JsonContent || !$content->isValid() ) {
			// This will fail anyway before saving.
			return;
		}
		$jsonSchema = $content->getData()->value;
		$title = $title->getDBkey();
		$isExistingPage = $revCount > $sRevCount;
		BucketDatabase::createOrModifyTable( $title, $jsonSchema, $isExistingPage );
	}

	/** @inheritDoc */
	public function onScribuntoExternalLibraries( $engine, &$extraLibraries ): void {
		if ( $engine === 'lua' ) {
			$extraLibraries['mw.ext.bucket'] = LuaLibrary::class;
		}
	}

	/** @inheritDoc */
	public function onSidebarBeforeOutput( $skin, &$sidebar ): void {
		if ( $skin->getTitle()->inNamespaces( $this->getEnabledNamespaces() ) ) {
			$sidebar['TOOLBOX'][] = [
				'text' => $skin->msg( 'bucket-sidebar-action' )->text(),
				'href' => $skin->getTitle()->getLocalURL( 'action=bucket' ),
				'title' => 'Bucket',
				'id' => 'n-bucket'
			];
		}
	}

	/** @inheritDoc */
	public function onArticleFromTitle( $title, &$article, $context ): void {
		if ( $title->getNamespace() !== NS_BUCKET ) {
			return;
		}
		$article = new BucketPage( $title );
	}

	/** @inheritDoc */
	public function onPageDelete(
		ProperPageIdentity $page, Authority $deleter, string $reason, StatusValue $status, bool $suppress
	): bool {
		if ( $page->getNamespace() !== NS_BUCKET ) {
			return true;
		}

		try {
			$pagesCount = BucketDatabase::countPagesUsingBucket( $page->getDBkey() );
			if ( $pagesCount === 0 ) {
				return true;
			} else {
				$status->fatal( 'bucket-delete-fail-in-use', $pagesCount );
				return false;
			}
		// If we somehow get a page that isn't a valid Bucket name, it will throw a schema exception.
		} catch ( BucketException $e ) {
			$status->warning( $e->getwfMessage() );
			return true;
		}
	}

	/** @inheritDoc */
	public function onPageDeleteComplete( ProperPageIdentity $page, Authority $deleter, string $reason, int $pageID,
	  RevisionRecord $deletedRev, ManualLogEntry $logEntry, int $archivedRevisionCount
	): void {
		if ( $page->getNamespace() !== NS_BUCKET ) {
			Bucket::writePuts( $page->getId(), '', [] );
		} else {
			try {
				BucketDatabase::deleteTable( $page->getDBkey() );
				// If we somehow get a page that isn't a valid Bucket name, it will throw a schema exception.
			} catch ( BucketException ) {

			}
		}
	}

	/** @inheritDoc */
	public function onPageMoveComplete( $old, $new, $user, $pageid, $redirid, $reason, $revision ): void {
		$enabledNamespaces = $this->getEnabledNamespaces();
		if ( in_array( $old->getNamespace(), $enabledNamespaces, true )
			&& !in_array( $new->getNamespace(), $enabledNamespaces, true ) ) {
			Bucket::writePuts( $pageid, '', [] );
		}
	}

	/** @inheritDoc */
	public function onContentModelCanBeUsedOn( $contentModel, $title, &$ok ): bool {
		if ( $title->getNamespace() === NS_BUCKET && $contentModel !== 'json' ) {
			$ok = false;
			return false;
		}
		return true;
	}

	/** @inheritDoc */
	public function onBeforeDisplayNoArticleText( $article ): bool {
		if ( $article->getTitle()->getNamespace() !== NS_BUCKET ) {
			return true;
		}

		if ( strtolower( $article->getTitle()->getRootTitle()->getDBkey() ) === Bucket::ISSUES_BUCKET ) {
			return false;
		}

		return true;
	}

	/** @inheritDoc */
	public function onTitleIsAlwaysKnown( $title, &$isKnown ): void {
		if ( $title->getNamespace() !== NS_BUCKET ) {
			return;
		}

		if ( strtolower( $title->getRootTitle()->getDBkey() ) === Bucket::ISSUES_BUCKET ) {
			$isKnown = true;
		}
	}

	/** @inheritDoc */
	public function onParserClearState( $parser ): void {
		LuaLibrary::clearCache();
		BucketQuery::clearCache();
	}

	/** @inheritDoc */
	public function onParserLimitReportPrepare( $parser, $output ): void {
		$maxTime = $this->config->get( 'BucketMaxPageExecutionTime' );
		$output->setLimitReportData( 'bucket-limitreport-run-time', [
				// Milliseconds to seconds
				sprintf( '%.3f', LuaLibrary::getPageElapsedTime() / 1000 ),
				// Strip trailing .0s
				rtrim( rtrim( sprintf( '%.3f', $maxTime / 1000 ), '0' ), '.' )
		] );
	}
}
