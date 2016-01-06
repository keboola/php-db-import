<?php
/**
 * Import CSV to Mysql database table
 *
 * CSV must contain header with column names
 * CSV columns are matched to database table column names by name
 *
 * All columns in database table must be present in csv except of internal columns begining with underscore
 * character!
 *
 * Static values import:
 *   You can load static values into specified columns for each row
 * 	 Example:
 *   $import->setStaticValues(array('storageApiTransaction' => 45646513))
 * 		->import('orders', $csvFile);
 *
 *
 *
 * User: Martin Halamíček
 * Date: 12.4.12
 * Time: 15:24
 *
 */

namespace Keboola\Db\Import;

use Keboola\Csv\CsvFile;
use Tracy\Debugger;


abstract class RedshiftBase implements ImportInterface
{

	protected  $connection;

	protected  $warnings = array();

	private $importedRowsCount = 0;

	private $timers = array();

	private $importedColumns = array();

	private $ignoreLines = 0;

	private $incremental = false;

	private $schemaName;

	public function __construct(\PDO $connection, $schemaName)
	{
		$this->connection = $connection;
		$this->schemaName = $schemaName;
	}


	/**
	 * @param $tableName
	 * @param $columns
	 * @param array CsvFile $csvFiles
	 * @return mixed
	 */
	public function import($tableName, $columns, array $sourceData)
	{
		$this->validateColumns($tableName, $columns);
		$stagingTableName = $this->createTableFromSourceTable($tableName);

		try {
			$this->importDataToStagingTable($stagingTableName, $columns, $sourceData);

			if ($this->getIncremental()) {
				$this->insertOrUpdateTargetTable(
					$stagingTableName,
					$tableName,
					$columns);
			} else {
				Debugger::timer('dedup');
				$this->dedup($stagingTableName, $columns, $this->getTablePrimaryKey($tableName));
				$this->addTimer('dedup', Debugger::timer('dedup'));
				$this->insertAllIntoTargetTable($stagingTableName, $tableName, $columns);
			}
			$this->dropTable($stagingTableName);
			$this->importedColumns = $columns;

			return new Result(array(
				'warnings' => $this->warnings,
				'timers' => $this->timers,
				'importedRowsCount' => $this->importedRowsCount,
				'importedColumns' => $this->importedColumns,
			));

		} catch (\Exception $e) {
			$this->dropTable($stagingTableName);
			throw $e;
		}

	}

	protected abstract function importDataToStagingTable($stagingTableName, $columns, $sourceData);


	private function validateColumns($tableName, $columnsToImport)
	{
		if (count($columnsToImport) == 0) {
			throw new Exception('No columns found in CSV file.', Exception::NO_COLUMNS,
				null, 'csvImport.noColumns');
		}

		$tableColumns = $this->getTableColumns($tableName);
		$columnsToImport = array_map('strtolower', $columnsToImport);

		$moreColumns = array_diff($columnsToImport, $tableColumns);
		if (!empty($moreColumns)) {
			throw new Exception('Columns doest not match', Exception::COLUMNS_COUNT_NOT_MATCH);
		}
	}

	private function insertAllIntoTargetTable($stagingTableName, $targetTableName, $columns)
	{
		$this->connection->beginTransaction();


		$targetTableNameWithSchema = $this->nameWithSchemaEscaped($targetTableName);
		$stagingTableNameWithSchema = $this->nameWithSchemaEscaped($stagingTableName);

		$this->query('TRUNCATE TABLE ' . $targetTableNameWithSchema);

		$columnsSql = implode(', ', array_map(function($column) {
            return $this->quoteIdentifier($column);
        }, $columns));

		$now = $this->getNowFormatted();
		if (in_array('_timestamp', $columns)) {
			$sql = "INSERT INTO {$targetTableNameWithSchema} ($columnsSql) (SELECT $columnsSql FROM $stagingTableNameWithSchema)";
		} else {
			$sql = "INSERT INTO {$targetTableNameWithSchema} ($columnsSql, _timestamp) (SELECT $columnsSql, '{$now}' FROM $stagingTableNameWithSchema)";
		}

		Debugger::timer('copyFromStagingToTarget');
		$this->query($sql);
		$this->addTimer('copyFromStagingToTarget', Debugger::timer('copyFromStagingToTarget'));

		$this->connection->commit();
	}

	/**
	 * Performs merge operation according to http://docs.aws.amazon.com/redshift/latest/dg/merge-specify-a-column-list.html
	 * @param $stagingTableName
	 * @param $targetTableName
	 * @param $columns
	 */
	private function insertOrUpdateTargetTable($stagingTableName, $targetTableName, $columns)
	{
		$this->connection->beginTransaction();
		$nowFormatted = $this->getNowFormatted();

		$targetTableNameWithSchema = $this->nameWithSchemaEscaped($targetTableName);
		$stagingTableNameWithSchema = $this->nameWithSchemaEscaped($stagingTableName);

		$primaryKey = $this->getTablePrimaryKey($targetTableName);

		if (!empty($primaryKey)) {

			// Update target table
			$sql = "UPDATE " . $targetTableNameWithSchema . " SET ";

			$columnsSet = [];
			foreach ($columns as $columnName) {
				$columnsSet[] = sprintf(
					"%s = %s.%s",
					$this->quoteIdentifier($columnName),
					$stagingTableNameWithSchema,
					$this->quoteIdentifier($columnName)
				);
			}

			$sql .= implode(', ', $columnsSet) . ", _timestamp = '{$nowFormatted}' ";
			$sql .= " FROM " . $stagingTableNameWithSchema . " ";
			$sql .= " WHERE ";

			$pkWhereSql = [];
			foreach ($primaryKey as $pkColumn) {
				$pkWhereSql[] = sprintf(
					"%s.%s = %s.%s",
					$targetTableNameWithSchema,
					$this->quoteIdentifier($pkColumn),
					$stagingTableNameWithSchema,
                    $this->quoteIdentifier($pkColumn)
				);
			}

			$sql .= implode(' AND ', $pkWhereSql) . " ";

			// update only changed rows - mysql TIMESTAMP ON UPDATE behaviour simulation
			$columnsComparsionSql = array_map(function($columnName) use($targetTableNameWithSchema, $stagingTableNameWithSchema) {
				return sprintf(
					"%s.%s != %s.%s",
					$targetTableNameWithSchema,
					$this->quoteIdentifier($columnName),
					$stagingTableNameWithSchema,
					$this->quoteIdentifier($columnName)
				);
			}, $columns);
			$sql .= " AND (" . implode(' OR ', $columnsComparsionSql). ") ";

			Debugger::timer('updateTargetTable');
			$this->query($sql);
			$this->addTimer('updateTargetTable', Debugger::timer('updateTargetTable'));

			// Delete updated rows from staging table
			$sql = "DELETE FROM " . $stagingTableNameWithSchema . " ";
			$sql .= "USING " . $targetTableNameWithSchema . " ";
			$sql .= "WHERE " . implode(' AND ', $pkWhereSql) ;

			Debugger::timer('deleteUpdatedRowsFromStaging');
			$this->query($sql);
			$this->addTimer('deleteUpdatedRowsFromStaging', Debugger::timer('deleteUpdatedRowsFromStaging'));

			// Dedup staging table
			Debugger::timer('dedupStaging');
			$this->dedup($stagingTableName, $columns, $primaryKey);
			$this->addTimer('dedupStaging', Debugger::timer('dedupStaging'));
		}

		// Insert from staging to target table
		$sql = "INSERT INTO " . $targetTableNameWithSchema. " (" . implode(', ', array_map(function($column) {
			return $this->quoteIdentifier($column);
		}, $columns)) . ", _timestamp) ";


		$columnsSetSql = [];

		foreach ($columns as $columnName) {
			$columnsSetSql[] = sprintf(
				"%s.%s",
				$stagingTableNameWithSchema,
				$this->quoteIdentifier($columnName)
			);
		}

		$sql .= "SELECT " . implode(',', $columnsSetSql) . ", '{$nowFormatted}' ";
		$sql .= "FROM " . $stagingTableNameWithSchema;
		Debugger::timer('insertIntoTargetFromStaging');
		$this->query($sql);
		$this->addTimer('insertIntoTargetFromStaging', Debugger::timer('insertIntoTargetFromStaging'));

		$this->connection->commit();
	}

	private  function replaceTables($sourceTableName, $targetTableName)
	{
		$this->dropTable($targetTableName);
		$this->query("ALTER TABLE {$this->nameWithSchemaEscaped($sourceTableName)} RENAME TO {$targetTableName}");
	}

	private function dropTable($tableName)
	{
		$this->query("DROP TABLE " . $this->nameWithSchemaEscaped($tableName));
	}

	protected function nameWithSchemaEscaped($tableName, $schemaName = null)
	{
		if ($schemaName === null) {
			$schemaName = $this->schemaName;
		}
		$tableNameFiltered = preg_replace('/[^a-zA-Z0-9_\-\.]+/', "", $tableName);
		return "\"{$schemaName}\".\"{$tableNameFiltered}\"";
	}

	private function uniqueValue()
	{
		return str_replace('.', '_', uniqid('csvimport', true));
	}

	private function dedup($tableName, $columns, array $primaryKey)
	{
		if (empty($primaryKey)) {
			return;
		}

        $pkSql = implode(',', array_map(function($column) {
            return $this->quoteIdentifier($column);
        }, $primaryKey));

		$sql = "SELECT ";

		$sql .= implode(",", array_map(function($column) {
			return "a." . $this->quoteIdentifier($column);
		}, $columns));

		$sql .= sprintf(" FROM (SELECT %s, ROW_NUMBER() OVER (PARTITION BY %s ORDER BY %s) AS \"row_number\" FROM %s)",
			implode(",", array_map(function($column) {
                return $this->quoteIdentifier($column);
            },$columns)),
			$pkSql,
			$pkSql,
			$this->nameWithSchemaEscaped($tableName)
		);

		$sql .= " AS a WHERE a.\"row_number\" = 1";
		$columnsSql = implode(', ', array_map(function($column) {
            return $this->quoteIdentifier($column);
        }, $columns));

		$tempTable = $this->createTableFromSourceTable($tableName);

		$this->query("INSERT INTO {$this->nameWithSchemaEscaped($tempTable)} ($columnsSql) " . $sql);
		$this->replaceTables($tempTable, $tableName);
	}

	private function createTableFromSourceTable($sourceTableName)
	{
		$tempName = '__temp_' . $this->uniqueValue();
		$this->query(sprintf(
			'CREATE TABLE %s (LIKE %s)',
			$this->nameWithSchemaEscaped($tempName),
			$this->nameWithSchemaEscaped($sourceTableName)
		));

		// PK is not copied - add it to the table
		$primaryKey = $this->getTablePrimaryKey($sourceTableName);
		$tableIdentifier = $this->nameWithSchemaEscaped($tempName);
		if (!empty($primaryKey)) {
			$this->query(sprintf(
				"
					ALTER TABLE %s
					ADD PRIMARY KEY (%s)
				",
				$tableIdentifier,
				implode(',', $primaryKey)
			));
		}

		return $tempName;
	}

	/**
	 * @param $tableName
	 * @return array
	 */
	private function getTablePrimaryKey($tableName)
	{
		$sql = sprintf("
			SELECT pa.attname FROM pg_catalog.pg_index i
				JOIN pg_catalog.pg_class ci on ci.oid = i.indexrelid
				JOIN pg_catalog.pg_class ct on ct.oid = i.indrelid
				JOIN pg_catalog.pg_attribute pa on pa.attrelid = ci.oid
				JOIN pg_catalog.pg_namespace pn on pn.oid = ct.relnamespace
				WHERE ct.relname = %s AND pn.nspname = %s
				AND i.indisprimary;
		", $this->connection->quote(strtolower($tableName)), $this->connection->quote(strtolower($this->schemaName)));

		return array_map(function($row) {
            return $row['attname'];
        }, $this->connection->query($sql)->fetchAll());
	}

	private function getTableColumns($tableName)
	{
		return array_map('strtolower', array_keys($this->describeTable(strtolower($tableName), strtolower($this->schemaName))));
	}


	protected function query($sql, $bind = array())
	{
		try {
			$this->connection->prepare($sql)->execute($bind);
		} catch (\PDOException $e) {
			if (strpos($e->getMessage(), 'Mandatory url is not present in manifest file') !== false) {
				throw new Exception('Mandatory url is not present in manifest file', Exception::MANDATORY_FILE_NOT_FOUND);
			}
			throw $e;
		}
	}

	/**
	 * @return string
	 */
	private function getNowFormatted()
	{
		$currentDate = new \DateTime('now', new \DateTimeZone('UTC'));
		return $currentDate->format('Ymd H:i:s');
	}

	/**
	 * @return bool
	 */
	public function getIncremental()
	{
		return $this->incremental;
	}

	/**
	 * @param $incremental
	 * @return CsvImportMysql
	 */
	public function setIncremental($incremental)
	{
		$this->incremental = (bool) $incremental;
		return $this;
	}

	/**
	 * @return boolean
	 */
	public function getIgnoreLines()
	{
		return $this->ignoreLines;
	}

	/**
	 * @param $linesCount
	 * @return $this
	 */
	public function setIgnoreLines($linesCount)
	{
		$this->ignoreLines = (int) $linesCount;
		return $this;
	}

	protected function addTimer($name, $value)
	{
		$this->timers[] = array(
			'name' => $name,
			'durationSeconds' => $value,
		);
	}

    protected function describeTable($tableName, $schemaName = null)
    {
        $sql = "SELECT
                a.attnum,
                n.nspname,
                c.relname,
                a.attname AS colname,
                t.typname AS type,
                a.atttypmod,
                FORMAT_TYPE(a.atttypid, a.atttypmod) AS complete_type,
                d.adsrc AS default_value,
                a.attnotnull AS notnull,
                a.attlen AS length,
                co.contype,
                ARRAY_TO_STRING(co.conkey, ',') AS conkey
            FROM pg_attribute AS a
                JOIN pg_class AS c ON a.attrelid = c.oid
                JOIN pg_namespace AS n ON c.relnamespace = n.oid
                JOIN pg_type AS t ON a.atttypid = t.oid
                LEFT OUTER JOIN pg_constraint AS co ON (co.conrelid = c.oid
                    AND a.attnum = ANY(co.conkey) AND co.contype = 'p')
                LEFT OUTER JOIN pg_attrdef AS d ON d.adrelid = c.oid AND d.adnum = a.attnum
            WHERE a.attnum > 0 AND c.relname = ".$this->connection->quote($tableName);
        if ($schemaName) {
            $sql .= " AND n.nspname = ".$this->connection->quote($schemaName);
        }
        $sql .= ' ORDER BY a.attnum';

        $stmt = $this->connection->query($sql);

        // Use FETCH_NUM so we are not dependent on the CASE attribute of the PDO connection
        $result = $stmt->fetchAll();

        $attnum        = 0;
        $nspname       = 1;
        $relname       = 2;
        $colname       = 3;
        $type          = 4;
        $atttypemod    = 5;
        $complete_type = 6;
        $default_value = 7;
        $notnull       = 8;
        $length        = 9;
        $contype       = 10;
        $conkey        = 11;

        $desc = array();
        foreach ($result as $key => $row) {
            $defaultValue = $row[$default_value];
            if ($row[$type] == 'varchar' || $row[$type] == 'bpchar' ) {
                if (preg_match('/character(?: varying)?(?:\((\d+)\))?/', $row[$complete_type], $matches)) {
                    if (isset($matches[1])) {
                        $row[$length] = $matches[1];
                    } else {
                        $row[$length] = null; // unlimited
                    }
                }
                if (preg_match("/^'(.*?)'::(?:character varying|bpchar)$/", $defaultValue, $matches)) {
                    $defaultValue = $matches[1];
                }
            }
            list($primary, $primaryPosition, $identity) = array(false, null, false);
            if ($row[$contype] == 'p') {
                $primary = true;
                $primaryPosition = array_search($row[$attnum], explode(',', $row[$conkey])) + 1;
                $identity = (bool) (preg_match('/^nextval/', $row[$default_value]));
            }
            $desc[$row[$colname]] = array(
                'SCHEMA_NAME'      => $row[$nspname],
                'TABLE_NAME'       => $row[$relname],
                'COLUMN_NAME'      => $row[$colname],
                'COLUMN_POSITION'  => $row[$attnum],
                'DATA_TYPE'        => $row[$type],
                'DEFAULT'          => $defaultValue,
                'NULLABLE'         => (bool) ($row[$notnull] != 't'),
                'LENGTH'           => $row[$length],
                'SCALE'            => null, // @todo
                'PRECISION'        => null, // @todo
                'UNSIGNED'         => null, // @todo
                'PRIMARY'          => $primary,
                'PRIMARY_POSITION' => $primaryPosition,
                'IDENTITY'         => $identity
            );
        }
        return $desc;
    }

    protected function quoteIdentifier($value)
    {
        $q = '"';
        return ($q . str_replace("$q", "$q$q", $value) . $q);
    }
}
