<?php

namespace MediaWiki\Extension\Bucket\Schema;

enum BucketValueType: string {
	case Text = 'TEXT';
	case Page = 'PAGE';
	case Double = 'DOUBLE';
	case Integer = 'INTEGER';
	case Boolean = 'BOOLEAN';
}
