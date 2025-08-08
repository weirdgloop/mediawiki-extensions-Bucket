<?php

namespace MediaWiki\Extension\Bucket;

use MediaWiki\Extension\Scribunto\Engines\LuaCommon\LibraryBase;
use MediaWiki\MediaWikiServices;
use MediaWiki\Title\MalformedTitleException;
use TypeError;
use Wikimedia\Rdbms\DBQueryTimeoutError;

class LuaLibrary extends LibraryBase {
	/**
	 * TODO Maybe we don't need to be static here? IDK the life cycle of this class
	 */
	private static int $pageElapsedTime = 0;
	private static array $linkedBuckets = [];

	public static function clearCache() {
		// Reset counter when we begin parsing a different page.
		self::$pageElapsedTime = 0;
		self::$linkedBuckets = [];
	}

	public static function getPageElapsedTime(): int {
		return self::$pageElapsedTime;
	}

	public function register() {
		$lib = [
			'put' => [ $this, 'bucketPut' ],
			'run' => [ $this, 'bucketRun' ],
		];
		return $this->getEngine()->registerInterface( __DIR__ . '/mw.ext.bucket.lua', $lib, [] );
	}

	/**
	 * @param array $builder
	 * @param array $data
	 * @return void
	 */
	public function bucketPut( $builder, $data ): void {
		$parserOutput = $this->getParser()->getOutput();
		$bucketName = $builder['bucketName'] ?? '';
		$sub = $builder['subversion'];
		$linkedBucketNames = $parserOutput->getExtensionData( Bucket::EXTENSION_BUCKET_NAMES_KEY ) ?? [];
		$parserOutput->appendExtensionData( Bucket::EXTENSION_BUCKET_NAMES_KEY, $bucketName );
		if ( !array_key_exists( $bucketName, $linkedBucketNames ) ) {
			try {
				// Add the Bucket page as a "template" used on this page.
				// This will get us linksUpdate scheduled for free when the Bucket page changes.
				$title = MediaWikiServices::getInstance()->getTitleParser()->parseTitle( $bucketName, NS_BUCKET );
				$bucketPage = MediaWikiServices::getInstance()->getWikiPageFactory()->newFromLinkTarget( $title );
				$bucketRevisionRecord = $bucketPage->getRevisionRecord();
				if ( $bucketRevisionRecord !== null ) {
					$parserOutput->addTemplate( $title, $bucketPage->getId(), $bucketRevisionRecord->getId() );
				}
			} catch ( MalformedTitleException ) {
				// Just ignore it, an error will be logged later
			}
			$bucketPuts[ $bucketName ] = [];
		}
		$uuid = MediaWikiServices::getInstance()->getGlobalIdGenerator()->newUUIDv4();
		$parserOutput->appendExtensionData( Bucket::EXTENSION_DATA_KEY, $uuid );
		$parserOutput->setExtensionData(
			Bucket::EXTENSION_DATA_KEY . $uuid,
			[ 'bucket' => $bucketName, 'sub' => $sub, 'data' => $data ]
		);
	}

	/**
	 * @param array $data
	 * @return array|array[]
	 */
	public function bucketRun( $data ): array {
		$startTime = hrtime( true );
		$maxTime = MediaWikiServices::getInstance()->getMainConfig()->get( 'BucketMaxPageExecutionTime' );
		if ( self::$pageElapsedTime > $maxTime ) {
			return [ 'error' => wfMessage( 'bucket-query-total-time-expired' )->text() ];
		}
		try {
			$this->linkToBucket( $data['bucketName'] );
			foreach ( $data['joins'] as $join ) {
				$this->linkToBucket( $join['bucketName'] );
			}
			$data = self::convertFromLuaTable( $data );
			$rows = Bucket::runSelect( $data );
			return [ self::convertToLuaTable( $rows ) ];
		} catch ( BucketException $e ) {
			return [ 'error' => $e->getMessage() ];
		} catch ( DBQueryTimeoutError ) {
			return [ 'error' => wfMessage( 'bucket-query-long-execution-time' )->text() ];
		} catch ( TypeError $e ) {
			return [ 'error' => wfMessage( 'bucket-php-type-error', $e->getMessage() )->text() ];
		} finally {
			// Convert nanoseconds to milliseconds
			self::$pageElapsedTime += (int)( ( hrtime( true ) - $startTime ) / 1000000 );
		}
	}

	/**
	 * Add the bucket page as a linked page.
	 * This allows Special:WhatLinksHere on Bucket pages to show a list of pages that read from that bucket.
	 * @param string $bucketName
	 */
	private function linkToBucket( $bucketName ) {
		if ( array_key_exists( $bucketName, self::$linkedBuckets ) ) {
			return;
		} else {
			self::$linkedBuckets[$bucketName] = true;
			$title = MediaWikiServices::getInstance()->getTitleParser()->parseTitle( $bucketName, NS_BUCKET );
			$this->getParser()->getOutput()->addLink( $title );
		}
	}

	/**
	 * Go from 0-index to 1-index.
	 * @param mixed $arr
	 * @return array
	 */
	public function convertToLuaTable( $arr ) {
		if ( is_array( $arr ) ) {
			$luaTable = [];
			foreach ( $arr as $key => $value ) {
				if ( is_int( $key ) || is_string( $key ) ) {
					$new_key = is_int( $key ) ? $key + 1 : $key;
					$luaTable[$new_key] = self::convertToLuaTable( $value );
				}
			}
			return $luaTable;
		}
		return $arr;
	}

	/**
	 * Go from 1-index to 0-index.
	 * @param mixed $arr
	 * @return array
	 */
	public static function convertFromLuaTable( $arr ) {
		if ( is_array( $arr ) ) {
			$luaTable = [];
			foreach ( $arr as $key => $value ) {
				if ( is_int( $key ) || is_string( $key ) ) {
					$new_key = is_int( $key ) ? $key - 1 : $key;
					$luaTable[$new_key] = self::convertFromLuaTable( $value );
				}
			}
			return $luaTable;
		}
		return $arr;
	}
}
