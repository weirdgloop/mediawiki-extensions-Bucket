<?php

namespace MediaWiki\Extension\Bucket\Query;

abstract class GroupNode extends QueryNode {
	protected array $children;

	public function __construct( array $children ) {
		$this->children = $children;
	}
}
