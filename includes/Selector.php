<?php

namespace MediaWiki\Extension\Bucket;

use Wikimedia\Rdbms\IDatabase;

abstract class Selector {
	protected bool $subquery = false;

	abstract public function getSafe( IDatabase $dbw ): string;

	abstract public function getUnsafe(): string;

	abstract public function getSelectSQL( IDatabase $dbw ): string;

	public function setSubquery( bool $subquery ): void {
		$this->subquery = $subquery;
	}
}
