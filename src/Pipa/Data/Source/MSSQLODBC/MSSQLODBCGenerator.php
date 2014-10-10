<?php

namespace Pipa\Data\Source\MSSQLODBC;
use DateTime;
use Pipa\Data\Collection;
use Pipa\Data\Field;
use Pipa\Data\Util\GenericSQLGenerator;
use Pipa\Data\Exception\QuerySyntaxException;

class MSSQLODBCSQLGenerator extends GenericSQLGenerator {
	
	protected $dataSource;
	
	function __construct(MSSQLODBCDataSource $dataSource) {
		$this->dataSource = $dataSource;
	}
	
	function escapeField(Field $field) {
		$escaped = $this->escapeIdentifier($field->name);
		if ($field->collection) {
			$escaped = $this->escapeIdentifier(
				$field->collection->alias
				? $field->collection->alias
				: $field->collection->name
			).".$escaped";
		}
		return $escaped;
	}

	function escapeIdentifier($name) {
		return "[$name]";
	}

	function escapeValue($value) {
		if (is_string($value))
			return "'".str_replace("'", "''", $value)."'";
		elseif ($value instanceof DateTime) {
			if ($value->getOffset() != 0) {
				$value = clone $value;
				$value->setTimezone(new DateTimeZone("UTC"));
			}
			return $this->escapeValue($value->format('Y-m-d H:i:s'));
		} elseif (is_bool($value))
			return $value ? "TRUE" : "FALSE";
		elseif (is_null($value))
			return "NULL";
		elseif (is_object($value))
			return $this->escapeValue((string) $value);
		else
			return $value;
	}
	
	function renderRegex($a, $b) {
		throw new QuerySyntaxException("Regular expressions not supported in ODBC");
	}

	function generateInsertHeaderComponents(array $fields, Collection $collection) {
		$components = parent::generateInsertHeaderComponents($fields, $collection);

		$escapedFields = array();
		foreach($fields as $field) {
			$escapedFields[] = $field;
		}
		$components['fields'] = '('.join(', ', $escapedFields).')';
		
		return $components;
	}	
}
