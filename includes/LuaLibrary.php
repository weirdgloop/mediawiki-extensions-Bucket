<?php

namespace MediaWiki\Extension\Bucket;

use MediaWiki\MediaWikiServices;

class LuaLibrary extends Scribunto_LuaLibraryBase {
	public function register() {
		$lib = [
			'put' => [ $this, 'bucketPut' ],
			'get' => [ $this, 'bucketGet' ],
			'run' => [ $this, 'bucketRun' ]
		];
		return $this->getEngine()->registerInterface( __DIR__ . '/../lua/bucket.lua', $lib, [] );
	}

	public function bucketPut($table_name, $data): void {
		$parserOutput = $this->getParser()->getOutput();
		if (!isset($parserOutput->bucketPuts)) {
			$parserOutput->bucketPuts = [];
		}
		if (!array_key_exists($table_name, $parserOutput->bucketPuts)) {
			$parserOutput->bucketPuts[ $table_name ] = [];
		}
		$parserOutput->bucketPuts[ $table_name ][] = $data;
	}

	public function bucketRun($data): array {
		$data = self::convertFromLuaTable($data);
		$rows = Bucket::runSelect($data);
		return [ self::convertToLuaTable($rows) ];
	}

	// Go from 0-index to 1-index.
	public function convertToLuaTable($arr) {
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
	public function convertFromLuaTable($arr) {
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
