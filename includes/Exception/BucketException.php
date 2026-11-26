<?php

namespace MediaWiki\Extension\Bucket\Exception;

use LogicException;
use MediaWiki\Message\Message;

class BucketException extends LogicException {
	private Message $wfMessage;

	public function __construct( Message $wfMessage ) {
		$this->wfMessage = $wfMessage->inContentLanguage();
		parent::__construct( $this->wfMessage );
	}

	public function getwfMessage(): Message {
		return $this->wfMessage;
	}
}
