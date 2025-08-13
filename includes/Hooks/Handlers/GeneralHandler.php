<?php

namespace MediaWiki\Extension\Bucket\Hooks\Handlers;

use Article;
use ManualLogEntry;
use MediaWiki\CommentStore\CommentStoreComment;
use MediaWiki\Content\Hook\ContentModelCanBeUsedOnHook;
use MediaWiki\Content\JsonContent;
use MediaWiki\Context\IContextSource;
use MediaWiki\Deferred\LinksUpdate\LinksUpdate;
use MediaWiki\Extension\Bucket\Bucket;
use MediaWiki\Extension\Bucket\BucketDatabase;
use MediaWiki\Extension\Bucket\BucketException;
use MediaWiki\Extension\Bucket\BucketPage;
use MediaWiki\Extension\Bucket\BucketQuery;
use MediaWiki\Extension\Bucket\LuaLibrary;
use MediaWiki\Extension\Scribunto\Hooks\ScribuntoExternalLibrariesHook;
use MediaWiki\Hook\AfterImportPageHook;
use MediaWiki\Hook\LinksUpdateCompleteHook;
use MediaWiki\Hook\ParserClearStateHook;
use MediaWiki\Hook\ParserLimitReportPrepareHook;
use MediaWiki\Hook\SidebarBeforeOutputHook;
use MediaWiki\Hook\TitleIsAlwaysKnownHook;
use MediaWiki\MediaWikiServices;
use MediaWiki\Page\Hook\ArticleFromTitleHook;
use MediaWiki\Page\Hook\BeforeDisplayNoArticleTextHook;
use MediaWiki\Page\Hook\PageDeleteCompleteHook;
use MediaWiki\Page\Hook\PageDeleteHook;
use MediaWiki\Page\Hook\PageUndeleteCompleteHook;
use MediaWiki\Page\Hook\PageUndeleteHook;
use MediaWiki\Page\ProperPageIdentity;
use MediaWiki\Parser\Parser;
use MediaWiki\Parser\ParserOutput;
use MediaWiki\Permissions\Authority;
use MediaWiki\Revision\RenderedRevision;
use MediaWiki\Revision\RevisionRecord;
use MediaWiki\Revision\SlotRecord;
use MediaWiki\Status\Status;
use MediaWiki\Storage\Hook\MultiContentSaveHook;
use MediaWiki\Title\Title;
use MediaWiki\User\UserIdentity;
use Skin;
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
	ParserLimitReportPrepareHook
{
	private BucketDatabase $bucketDb;

	public function __construct( BucketDatabase $bucketDb ) {
		$this->bucketDb = $bucketDb;
	}

	private function enabledNamespaces() {
		$config = MediaWikiServices::getInstance()->getMainConfig();
		return array_keys( $config->get( 'BucketWriteEnabledNamespaces' ), true );
	}

	/**
	 * @see https://www.mediawiki.org/wiki/Manual:Hooks/LinksUpdateComplete
	 *
	 * @param LinksUpdate $linksUpdate
	 * @param mixed $ticket
	 * @return void
	 */
	public function onLinksUpdateComplete( $linksUpdate, $ticket ) {
		if ( $linksUpdate->getTitle()->inNamespaces( $this->enabledNamespaces() ) ) {
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

	/**
	 * @see https://www.mediawiki.org/wiki/Manual:Hooks/PageUndelete
	 * @return bool
	 */
	public function onPageUndelete(
		ProperPageIdentity $page, Authority $performer, string $reason, bool $unsuppress, array $timestamps,
		array $fileVersions, StatusValue $status
	) {
		if ( $page->getNamespace() !== NS_BUCKET ) {
			return true;
		}
		$title = $page->getDBkey();
		try {
			if ( $this->bucketDb->canCreateTable( $title ) ) {
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

	/**
	 * @see https://www.mediawiki.org/wiki/Manual:Hooks/PageUndeleteComplete
	 */
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
		$this->bucketDb->createOrModifyTable( $title, $jsonSchema, $isExistingPage );
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
		$isExistingPage = $revRecord->getParentId() !== null;
		try {
			$this->bucketDb->createOrModifyTable( $title, $jsonSchema, $isExistingPage );
		} catch ( BucketException $e ) {
			$status->fatal( $e->getwfMessage() );
			return false;
		}
	}

	/**
	 * @see https://www.mediawiki.org/wiki/Manual:Hooks/AfterImportPage
	 *
	 * @param Title $title
	 * @param Title $foreignTitle
	 * @param int $revCount
	 * @param int $sRevCount
	 * @param array $pageInfo
	 */
	public function onAfterImportPage( $title, $foreignTitle, $revCount, $sRevCount, $pageInfo ) {
		if ( $title->getNamespace() !== NS_BUCKET ) {
			return;
		}
		$content = MediaWikiServices::getInstance()->getRevisionLookup()->getRevisionByTitle( $title )
			->getContent( SlotRecord::MAIN );
		if ( !$content instanceof JsonContent || !$content->isValid() ) {
			// This will fail anyway before saving.
			return;
		}
		$jsonSchema = $content->getData()->value;
		$title = $title->getDBkey();
		$isExistingPage = $revCount > $sRevCount;
		$this->bucketDb->createOrModifyTable( $title, $jsonSchema, $isExistingPage );
	}

	/**
	 * @see https://www.mediawiki.org/wiki/Manual:Hooks/ScribuntoExternalLibraries
	 *
	 * @param string $engine
	 * @param array &$extraLibraries
	 * @return void
	 */
	public function onScribuntoExternalLibraries( $engine, &$extraLibraries ) {
		if ( $engine === 'lua' ) {
			$extraLibraries['mw.ext.bucket'] = LuaLibrary::class;
		}
	}

	/**
	 * @param Skin $skin
	 * @param array &$sidebar
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
	 * $see https://www.mediawiki.org/wiki/Manual:Hooks/PageDelete
	 * @return bool
	 */
	public function onPageDelete(
		ProperPageIdentity $page, Authority $deleter, string $reason, StatusValue $status, bool $suppress
	) {
		if ( $page->getNamespace() !== NS_BUCKET ) {
			return true;
		}

		try {
			$pagesCount = $this->bucketDb->countPagesUsingBucket( $page->getDBkey() );
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

	/**
	 * @see https://www.mediawiki.org/wiki/Manual:Hooks/PageDeleteComplete
	 */
	public function onPageDeleteComplete( ProperPageIdentity $page, Authority $deleter, string $reason, int $pageID,
	  RevisionRecord $deletedRev, ManualLogEntry $logEntry, int $archivedRevisionCount
	) {
		if ( $page->getNamespace() !== NS_BUCKET ) {
			Bucket::writePuts( $page->getId(), '', [] );
		} else {
			try {
				$this->bucketDb->deleteTable( $page->getDBkey() );
				// If we somehow get a page that isn't a valid Bucket name, it will throw a schema exception.
			} catch ( BucketException ) {

			}
		}
	}

	/**
	 * @param string $contentModel
	 * @param Title $title
	 * @param bool &$ok
	 * @return bool
	 * @see https://www.mediawiki.org/wiki/Manual:Hooks/ContentModelCanBeUsedOn
	 */
	public function onContentModelCanBeUsedOn( $contentModel, $title, &$ok ) {
		if ( $title->getNamespace() === NS_BUCKET && $contentModel !== 'json' ) {
			$ok = false;
			return false;
		}
		return true;
	}

	/**
	 * @param Article $article
	 * @return bool
	 * @see https://www.mediawiki.org/wiki/Manual:Hooks/BeforeDisplayNoArticleText
	 */
	public function onBeforeDisplayNoArticleText( $article ) {
		if ( $article->getTitle()->getNamespace() !== NS_BUCKET ) {
			return true;
		}

		if ( strtolower( $article->getTitle()->getRootTitle()->getDBkey() ) === Bucket::ISSUES_BUCKET ) {
			return false;
		}

		return true;
	}

	/**
	 * @param Title $title
	 * @param bool &$isKnown
	 * @see https://www.mediawiki.org/wiki/Manual:Hooks/TitleIsAlwaysKnown
	 */
	public function onTitleIsAlwaysKnown( $title, &$isKnown ) {
		if ( $title->getNamespace() !== NS_BUCKET ) {
			return;
		}

		if ( strtolower( $title->getRootTitle()->getDBkey() ) === Bucket::ISSUES_BUCKET ) {
			$isKnown = true;
		}
	}

	/**
	 * @param Parser $parser
	 * @see https://www.mediawiki.org/wiki/Manual:Hooks/ParserClearState
	 */
	public function onParserClearState( $parser ) {
		LuaLibrary::clearCache();
		BucketQuery::clearCache();
	}

	/**
	 * @param Parser $parser
	 * @param ParserOutput $output
	 * @see https://www.mediawiki.org/wiki/Manual:Hooks/ParserLimitReportPrepare
	 */
	public function onParserLimitReportPrepare( $parser, $output ) {
		$maxTime = MediaWikiServices::getInstance()->getMainConfig()->get( 'BucketMaxPageExecutionTime' );
		$output->setLimitReportData( 'bucket-limitreport-run-time', [
				// Milliseconds to seconds
				sprintf( '%.3f', LuaLibrary::getPageElapsedTime() / 1000 ),
				// Strip trailing .0s
				rtrim( rtrim( sprintf( '%.3f', $maxTime / 1000 ), '0' ), '.' )
		] );
	}
}
