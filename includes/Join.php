<?php

namespace MediaWiki\Extension\Bucket;

use Wikimedia\Rdbms\IDatabase;
use Wikimedia\Rdbms\SelectQueryBuilder;

abstract class Join {
	abstract public function getSQL( IDatabase $dbw ): array;

	abstract public function getJoinTable( IDatabase $dbw ): string|SelectQueryBuilder;

	abstract public function getAlias(): string;
}
