<?php

namespace MediaWiki\Extension\Bucket;

use MediaWiki\Extension\Scribunto\Engines\LuaCommon\LibraryBase;
use MediaWiki\MediaWikiServices;
use MediaWiki\Title\MalformedTitleException;
use TypeError;
use Wikimedia\Rdbms\DBQueryTimeoutError;

class LuaLibrary extends LibraryBase {
	private static $elapsedTime = 0;
	private static $currentPage = null;

	public function register() {
		$lib = [
			'put' => [ $this, 'bucketPut' ],
			'run' => [ $this, 'bucketRun' ],
		];
		return $this->getEngine()->registerInterface( __DIR__ . '/mw.ext.bucket.lua', $lib, [] );
	}

	public function bucketPut( $builder, $data ): void {
		$parserOutput = $this->getParser()->getOutput();
		$bucketPuts = $parserOutput->getExtensionData( Bucket::EXTENSION_DATA_KEY ) ?? [];
		$bucketName = '';
		if ( array_key_exists( 'bucketName', $builder ) ) {
			$bucketName = $builder['bucketName'];
		}
		$sub = $builder['subversion'];
		if ( !array_key_exists( $bucketName, $bucketPuts ) ) {
			try {
				// Add the Bucket page as a "template" used on this page. This will get us linksUpdate scheduled for free when the Bucket page changes.
				$title = MediaWikiServices::getInstance()->getTitleParser()->parseTitle( $bucketName, NS_BUCKET );
				$bucketPage = MediaWikiServices::getInstance()->getWikiPageFactory()->newFromLinkTarget( $title );
				$bucketRevisionRecord = $bucketPage->getRevisionRecord();
				if ( $bucketRevisionRecord !== null ) {
					$parserOutput->addTemplate( $title, $bucketPage->getId(), $bucketRevisionRecord->getId() );
				}
			} catch ( MalformedTitleException $e ) {
				// Just ignore it, an error will be logged later
			}
			$bucketPuts[ $bucketName ] = [];
		}
		$bucketPuts[ $bucketName ][] = [ 'sub' => $sub, 'data' => $data ];
		$parserOutput->setExtensionData( Bucket::EXTENSION_DATA_KEY, $bucketPuts );
	}

	public function bucketRun( $data ): array {
		// Reset counter when we begin parsing a different page.
		if ( !isset( self::$currentPage ) || !$this->getParser()->getPage()->isSamePageAs( self::$currentPage ) ) {
			self::$currentPage = $this->getParser()->getPage();
			self::$elapsedTime = 0;
		}
		$startTime = hrtime( true );
		$maxTime = MediaWikiServices::getInstance()->getMainConfig()->get( 'BucketMaxPageExecutionTime' );
		if ( self::$elapsedTime > $maxTime ) {
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
		} catch ( DBQueryTimeoutError $e ) {
			return [ 'error' => wfMessage( 'bucket-query-long-execution-time' )->text() ];
		} catch ( TypeError $e ) {
			return [ 'error' => wfMessage( 'bucket-php-type-error', $e->getMessage() )->text() ];
		} finally {
			self::$elapsedTime += ( ( hrtime( true ) - $startTime ) / 1000000 ); // Convert nanoseconds to milliseconds
			$this->getParser()->getOutput()->setLimitReportData( 'bucket-limitreport-run-time', [
					sprintf( '%.3f', self::$elapsedTime / 1000 ), // Milliseconds to seconds
					// Strip trailing .0s
					rtrim( rtrim( sprintf( '%.3f', $maxTime / 1000 ), '0' ), '.' )
			] );
		}
	}

	/**
	 * Add the bucket page as a linked page. This allows Special:WhatLinksHere on Bucket pages to show a list of pages that read from that bucket.
	 */
	private function linkToBucket( $bucketName ) {
		$title = MediaWikiServices::getInstance()->getTitleParser()->parseTitle( $bucketName, NS_BUCKET );
		$this->getParser()->getOutput()->addLink( $title );
	}

	// Go from 0-index to 1-index.
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

	// Go from 1-index to 0-index.
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
