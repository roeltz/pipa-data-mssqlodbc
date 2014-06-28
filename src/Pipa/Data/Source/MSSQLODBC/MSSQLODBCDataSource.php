<?php

namespace Pipa\Data\Source\ODBC;
use DateTime;
use DateTimeZone;
use Pipa\Data\Aggregate;
use Pipa\Data\Collection;
use Pipa\Data\Criteria;
use Pipa\Data\DataSource;
use Pipa\Data\Exception\AuthException;
use Pipa\Data\Exception\ConnectionException;
use Pipa\Data\Exception\ConstraintException;
use Pipa\Data\Exception\DataException;
use Pipa\Data\Exception\DuplicateEntryException;
use Pipa\Data\Exception\InvalidHostException;
use Pipa\Data\Exception\QueryException;
use Pipa\Data\Exception\QuerySyntaxException;
use Pipa\Data\Exception\UnknownCollectionException;
use Pipa\Data\Exception\UnknownFieldException;
use Pipa\Data\Exception\UnknownHostException;
use Pipa\Data\Exception\UnknownSchemaException;
use Pipa\Data\JoinableCollection;
use Pipa\Data\MultipleInsertionSupport;
use Pipa\Data\RelationalCriteria;
use Pipa\Data\SQLDataSource;
use Pipa\Data\TransactionalDataSource;
use Pipa\Data\Util\AbstractConvenientSQLDataSource;
use Psr\Log\LoggerInterface;

class MSSQLODBCDataSource extends AbstractConvenientSQLDataSource implements DataSource, TransactionalDataSource, MultipleInsertionSupport {

	const TYPE_COUNTER = "COUNTER";
	const TYPE_VARCHAR = "VARCHAR";
	const TYPE_LONGCHAR = "LONGCHAR";
	const TYPE_INTEGER = "INTEGER";
	const TYPE_SMALLINT = "SMALLINT";
	const TYPE_BYTE = "BYTE";
	const TYPE_DOUBLE = "DOUBLE";
	const TYPE_REAL = "REAL";
	const TYPE_DECIMAL = "DECIMAL";
	const TYPE_DATETIME = "DATETIME";
	const TYPE_CURRENCY = "CURRENCY";
	const TYPE_BIT = "BIT";

	protected $connection;
	protected $generator;
	protected $logger;
	
	function __construct($dsn, $user, $password, $cursorType = null) {
		$this->connection = @odbc_pconnect($dsn, $user, $password, $cursorType);
		
		if ($this->connection) {
			$this->generator = new MSSQLODBCSQLGenerator($this);
		} else {
			throw $this->translateException(odbc_error(), odbc_errormsg());
		}
	}
	
	function getCollection($name) {
		return new JoinableCollection($name);
	}
	
	function getConnection() {
		return $this->connection;
	}
	
	function getCriteria() {
		return new RelationalCriteria($this);
	}
	
	function query($sql, array $parameters = null) {
		if ($parameters) $sql = $this->generator->interpolateParameters($sql, $parameters);
		
		if ($this->logger) {
			$this->logger->debug("$sql");
			$start = microtime(true);
		}
		
		$result = odbc_exec($this->connection, $sql);
		
		if ($result) {
			$types = $this->getResultTypes($result);
			$items = array();
			while($item = odbc_fetch_array($result)) {
				$this->processItem($item, $types);
				$items[] = $item;
			}

			if ($this->logger) {
				$elapsed = microtime(true) - $start;
				$count = count($items);
				$this->logger->debug("Query returned $count item(s), took {$elapsed}s");
			}
			
			return $items;
		} else {
			throw $this->translateException(odbc_error($this->connection), odbc_errormsg($this->connection));
		}
	}
	
	function execute($sql, array $parameters = null) {
		if ($parameters) $sql = $this->generator->interpolateParameters($sql, $parameters);
		
		if ($this->logger) {
			$this->logger->debug("$sql");
			$start = microtime(true);
		}
		
		$result = @odbc_exec($this->connection, $sql);
		
		if ($result) {
			$rows = odbc_num_rows($result);

			if ($this->logger) {
				$elapsed = microtime(true) - $start;
				$count = count($items);
				$this->logger->debug("$rows affected row(s), took {$elapsed}s");
			}
			
			return $rows;
		} else {
			throw $this->translateException(odbc_error($this->connection), odbc_errormsg($this->connection));
		}		
	}
	
	function find(Criteria $criteria) {
		return $this->query($this->generator->generateSelect($criteria));
	}
	
	function count(Criteria $criteria) {
		$result = $this->query($this->generator->generateCount($criteria));
		return current(current($result));
	}
	
	function aggregate(Aggregate $aggregate, Criteria $criteria) {
		$result = $this->query($this->generator->generateAggregate($aggregate, $criteria));
		return current(current($result));
	}
	
	function save(array $values, Collection $collection, $sequence = null) {
		$this->execute($this->generator->generateInsert($values, $collection));
		$id = $this->query("SELECT @@IDENTITY AS ID");
		$id = current(current($id));
		if ($id) return $id;
	}
	
	function update(array $values, Criteria $criteria) {
		return $this->execute($this->generator->generateUpdate($values, $criteria));
	}
	
	function delete(Criteria $criteria) {
		return $this->execute($this->generator->generateDelete($criteria));
	}

	function beginTransaction() {
		$this->execute("BEGIN TRANSACTION");
	}
	
	function commit() {
		$this->execute("COMMIT TRANSACTION");
	}
	
	function rollback() {
		$this->execute("ROLLBACK TRANSACTION");	
	}

	function saveMultiple(array $values, Collection $collection) {
		$this->execute($this->generator->generateMultipleInsert($values, $collection));
	}	

	function setLogger(LoggerInterface $logger) {
		$this->logger = $logger;
	}
	
	protected function getResultTypes($result) {
		$types = array();
		for ($i = 1, $n = odbc_num_fields($result); $i <= $n; $i++) {
			$types[odbc_field_name($result, $i)] = odbc_field_type($result, $i);
		}
		return $types;
	}
	
	protected function processItem(array &$item, array $types) {
		foreach($item as $field=>&$value) {
			if (!is_null($value)) {
				
				switch($types[$field]) {
					case self::TYPE_BYTE:
					case self::TYPE_COUNTER:
					case self::TYPE_INTEGER:
					case self::TYPE_SMALLINT:
						$value = (int) $value;
						break;
					case self::TYPE_CURRENCY:
					case self::TYPE_DECIMAL:
					case self::TYPE_REAL:
					case self::TYPE_DOUBLE:
						$value = (double) $value;
						break;
					case self::TYPE_BIT:
						$value = $value == "1";
						break;
					case self::TYPE_DATETIME:
						$value = new DateTime($value, new DateTimeZone("UTC"));
						break;
				}
			}
		}
	}
	
	protected function translateException($error, $msg) {
		//print_r(compact("error", "msg"));
		switch($error) {
			case "IM002":
				return new ConnectionException("Data source name not found and no default driver specified: $msg");
			case "IM003":
				return new ConnectionException("Specified driver could not be loaded: $msg");
			case "IM014":
				return new ConnectionException("Invalid name of File DSN: $msg");
			case "IM015":
				return new ConnectionException("Corrupt file data source: $msg");
			case "42000":
				return new AuthException("Syntax error or access violation: $msg");
			case "S1000":
			case "23000":
				return new ConstraintException($msg);
			case "S0002":
				return new UnknownCollectionException("Table not valid: $msg");
			case "07001":
				return new UnknownFieldException("Field not valid: $msg");
			default:
				return new QueryException($msg);
		}
	}
}
