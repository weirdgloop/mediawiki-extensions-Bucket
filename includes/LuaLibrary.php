<?php

namespace MediaWiki\Extension\Bucket;

use MediaWiki\Extension\Scribunto\Engines\LuaCommon\LibraryBase;

class LuaLibrary extends LibraryBase {
	public function register() {
		$lib = [
			'put' => [ $this, 'bucketPut' ],
			'get' => [ $this, 'bucketGet' ],
			'run' => [ $this, 'bucketRun' ]
		];
		return $this->getEngine()->registerInterface( __DIR__ . '/mw.ext.bucket.lua', $lib, [] );
	}

	public function bucketPut( $table_name, $data ): void {
		$parserOutput = $this->getParser()->getOutput();
		$bucketPuts = $parserOutput->getExtensionData( Bucket::EXTENSION_DATA_KEY ) ?? [];
		if ( !array_key_exists( $table_name, $bucketPuts ) ) {
			$bucketPuts[ $table_name ] = [];
		}
		$bucketPuts[ $table_name ][] = $data;
		$parserOutput->setExtensionData( Bucket::EXTENSION_DATA_KEY, $bucketPuts );
	}

	public function bucketRun( $data ): array {
		$data = self::convertFromLuaTable( $data );
		$rows = Bucket::runSelect( $data );
		return [ self::convertToLuaTable( $rows ) ];
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
	public function convertFromLuaTable( $arr ) {
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
