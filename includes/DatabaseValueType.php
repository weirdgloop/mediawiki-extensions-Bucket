<?php

namespace MediaWiki\Extension\Bucket;

enum DatabaseValueType: string {
	case Text = 'TEXT';
	case Double = 'DOUBLE';
	case Integer = 'INTEGER';
	case Boolean = 'BOOLEAN';
	case Json = 'JSON';
}
