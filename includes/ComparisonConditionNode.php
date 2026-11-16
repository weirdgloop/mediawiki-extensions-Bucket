<?php

namespace MediaWiki\Extension\Bucket;

use Wikimedia\Rdbms\IDatabase;
use Wikimedia\Rdbms\IExpression;

class ComparisonConditionNode extends QueryNode {
	private Selector $selector;
	private Operator $operator;
	private Value $value;

	public function __construct( Selector $selector, Operator $operator, Value $value ) {
		$this->selector = $selector;
		$this->operator = $operator;
		$this->value = $value;
	}

	public function getWhereSQL( IDatabase $dbw ): IExpression {
		$selector = $this->selector;
		$op = $this->operator->getOperator();
		$value = $this->value->getValue();
		// If we are comparing a value with a text field, convert the value to a string
		// this prevents MySQL from trying to convert every row of the field to a different type
		if ( $selector instanceof FieldSelector
			&& $selector->getFieldSchema()->getSubDatabaseValueType() == DatabaseValueType::Text
			&& $value !== null ) {
			$value = strval( $value );
		}
		return $dbw->expr( $selector->getUnsafe(), $op, $value );
	}

	public function getSelector(): Selector {
		return $this->selector;
	}

	public function getValue(): Value {
		return $this->value;
	}
}
