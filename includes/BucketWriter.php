<?php

namespace MediaWiki\Extension\Bucket;

use MediaWiki\MediaWikiServices;

class BucketWriter {
	/**
	 * Cannot be static because RefreshLinks job will run on multiple pages
	 */
	private array $logs = [];

	public function logIssue( string $bucket, string $property, string $type, string $message ): void {
		if ( count( $this->logs ) > 100 ) {
			return;
		}
		if ( $bucket !== '' ) {
			$bucket = 'Bucket:' . $bucket;
		}
		$data = [
			'bucket' => $bucket,
			'property' => $property,
			'type' => wfMessage( $type ),
			'message' => $message
		];
		$hash = md5( json_encode( $data ) );
		$this->logs[$hash] = [
			'sub' => '',
			'data' => $data
		];
	}

	/**
	 * Called when a page is saved in a Bucket enabled namespace
	 */
	public function writePuts( int $pageId, string $titleText, array $puts, bool $writingLogs = false ): void {
		$dbw = BucketDatabase::getDB();
		$putLength = 0;
		$maxiumPutLength = MediaWikiServices::getInstance()->getMainConfig()->get( 'BucketMaxDataPerPage' );

		$res = $dbw->newSelectQueryBuilder()
				->from( 'bucket_pages' )
				->select( [ 'bucket_name', 'put_hash' ] )
				->forUpdate()
				->where( [ '_page_id' => $pageId ] )
				->caller( __METHOD__ )
				->fetchResultSet();
		$bucket_hash = [];
		foreach ( $res as $row ) {
			$bucket_hash[ $row->bucket_name ] = $row->put_hash;
		}

		$schemas = [];

		if ( count( $puts ) > 0 ) {
			// Combine existing written bucket list and new written bucket list.
			$relevantBuckets = array_merge( array_keys( $puts ), array_keys( $bucket_hash ) );
			$res = $dbw->newSelectQueryBuilder()
					->from( 'bucket_schemas' )
					->select( [ 'bucket_name', 'schema_json' ] )
					->lockInShareMode()
					->where( [ 'bucket_name' => $relevantBuckets ] )
					->caller( __METHOD__ )
					->fetchResultSet();
			foreach ( $res as $row ) {
				$schemas[$row->bucket_name] = new BucketSchema(
					$row->bucket_name, json_decode( $row->schema_json, true ) );
			}
		}

		// Batched data to write to bucket_pages
		$newPutHashes = [];
		foreach ( $puts as $bucketName => $bucketData ) {
			if ( $bucketName === '' ) {
				self::logIssue(
					$bucketName, '', 'bucket-general-error', wfMessage( 'bucket-no-bucket-defined-warning' ) );
				continue;
			}

			try {
				$bucketNameTmp = Bucket::getValidFieldName( $bucketName );
			} catch ( SchemaException ) {
				self::logIssue(
					$bucketName, '', 'bucket-general-warning', wfMessage(
						'bucket-invalid-name-warning', $bucketName ) );
				continue;
			}

			if ( $bucketNameTmp !== $bucketName ) {
				self::logIssue(
					$bucketName, '', 'bucket-general-warning', wfMessage( 'bucket-capital-name-warning' ) );
			}
			$bucketName = $bucketNameTmp;

			if ( $bucketName === Bucket::ISSUES_BUCKET && $writingLogs === false ) {
				self::logIssue(
					$bucketName, Bucket::ISSUES_BUCKET, 'bucket-general-error', wfMessage(
						'bucket-cannot-write-to-system-bucket' ) );
				continue;
			}

			if ( !array_key_exists( $bucketName, $schemas ) ) {
				self::logIssue( $bucketName, '', 'bucket-general-error', wfMessage( 'bucket-no-exist-error' ) );
				continue;
			}
			$bucketSchema = $schemas[$bucketName];

			$tablePuts = [];
			$dbTableName = BucketDatabase::getBucketTableName( $bucketName );
			$res = $dbw->newSelectQueryBuilder()
				->from( $dbTableName )
				->select( '*' )
				->forUpdate()
				->where( [ '_page_id' => $pageId ] )
				->caller( __METHOD__ )
				->fetchResultSet();

			$fields = [];
			$fieldNames = $res->getFieldNames();
			foreach ( $fieldNames as $fieldName ) {
				// If the table has a field that isn't present in the schema, the schema must be out of date.
				if ( !isset( $bucketSchema->getFields()[$fieldName] ) ) {
					self::logIssue(
						$bucketName, $fieldName, 'bucket-general-error', wfMessage( 'bucket-schema-outdated-error' ) );
				} else {
					$fields[$fieldName] = true;
				}
			}
			foreach ( $bucketData as $idx => $singleData ) {
				$sub = $singleData['sub'];
				$singleData = $singleData['data'];
				if ( !is_array( $singleData ) ) {
					self::logIssue( $bucketName, '', 'bucket-general-error', wfMessage( 'bucket-put-syntax-error' ) );
					continue;
				}
				foreach ( $singleData as $key => $value ) {
					if ( !isset( $fields[$key] ) || !$fields[$key] ) {
						self::logIssue(
							$bucketName, $key, 'bucket-general-warning', wfMessage(
								'bucket-put-key-missing-warning', $key, $bucketName ) );
					}
				}
				$singlePut = [];
				foreach ( $fields as $key => $_ ) {
					$value = $singleData[$key] ?? null;
					try {
						$singlePut[$dbw->addIdentifierQuotes( $key )] =
							$bucketSchema->getField( $key )->castValueForDatabase( $value );
					} catch ( BucketException $e ) {
						$singlePut[$dbw->addIdentifierQuotes( $key )] = null;
						self::logIssue(
							$bucketName, $key, 'bucket-general-error', $e->getMessage() );
					}
				}
				$singlePut[$dbw->addIdentifierQuotes( '_page_id' )] = $pageId;
				$singlePut[$dbw->addIdentifierQuotes( '_index' )] = $idx;
				$singlePut[$dbw->addIdentifierQuotes( 'page_name' )] = $titleText;
				$singlePut[$dbw->addIdentifierQuotes( 'page_name_sub' )] = $titleText;
				if ( isset( $sub ) && strlen( $sub ) > 0 ) {
					$singlePut[$dbw->addIdentifierQuotes( 'page_name_sub' )] = $titleText . '#' . $sub;
				}
				$tablePuts[$idx] = $singlePut;
			}

			# Check these puts against the hash of the last time we did puts.
			$tableJson = json_encode( $tablePuts );
			$putLength += strlen( $tableJson );
			$newHash = hash( 'sha256', $tableJson . json_encode( $bucketSchema ) );
			if ( isset( $bucket_hash[ $bucketName ] ) && $bucket_hash[ $bucketName ] === $newHash ) {
				unset( $bucket_hash[ $bucketName ] );
				continue;
			}

			// Remove the bucket_hash entry so we can use $bucket_hash as a list of removed buckets at the end.
			unset( $bucket_hash[ $bucketName ] );
			if ( $putLength <= $maxiumPutLength || $writingLogs ) {
				$newPutHashes[$bucketName] =
					[ '_page_id' => $pageId, 'bucket_name' => $bucketName, 'put_hash' => $newHash ];

				$dbw->newDeleteQueryBuilder()
					->deleteFrom( $dbw->addIdentifierQuotes( $dbTableName ) )
					->where( [ '_page_id' => $pageId ] )
					->caller( __METHOD__ )
					->execute();
				$dbw->newInsertQueryBuilder()
					->insert( $dbw->addIdentifierQuotes( $dbTableName ) )
					->rows( $tablePuts )
					->caller( __METHOD__ )
					->execute();
			}
		}

		// Insert new/updated hashes to bucket_pages
		if ( count( $newPutHashes ) > 0 ) {
			$dbw->newReplaceQueryBuilder()
				->replaceInto( 'bucket_pages' )
				->uniqueIndexFields( [ '_page_id', 'bucket_name' ] )
				->rows( array_values( $newPutHashes ) )
				->caller( __METHOD__ )
				->execute();
		}

		if ( $writingLogs ) {
			return;
		}

		if ( $putLength > $maxiumPutLength ) {
			self::logIssue( $bucketName, '', 'bucket-general-error',
				wfMessage( 'bucket-put-total-too-long' )->numParams( $putLength, $maxiumPutLength )
			);
		}

		if ( count( $this->logs ) > 0 ) {
			self::writePuts( $pageId, $titleText, [ Bucket::ISSUES_BUCKET => array_values( $this->logs ) ], true );
			unset( $bucket_hash[Bucket::ISSUES_BUCKET] );
		}

		// Clean up bucket_pages entries for buckets that are no longer written to on this page.
		$tablesToDelete = array_keys( $bucket_hash );
		if ( count( $tablesToDelete ) > 0 ) {
			$dbw->newDeleteQueryBuilder()
				->deleteFrom( 'bucket_pages' )
				->where( [ '_page_id' => $pageId, 'bucket_name' => $tablesToDelete ] )
				->caller( __METHOD__ )
				->execute();
			foreach ( $tablesToDelete as $name ) {
				$dbw->newDeleteQueryBuilder()
					->deleteFrom( BucketDatabase::getBucketTableName( $name ) )
					->where( [ '_page_id' => $pageId ] )
					->caller( __METHOD__ )
					->execute();
			}
		}
	}
}
