<?php
/**
 * Created by PhpStorm.
 * User: martinhalamicek
 * Date: 27/04/16
 * Time: 10:05
 */


namespace Keboola\Db\Import\Snowflake;

use Keboola\Db\Import\ImportInterface;
use Keboola\Db\Import\Exception;
use Tracy\Debugger;
use Keboola\Db\Import\Result;


abstract class ImportBase implements ImportInterface
{

    /**
     * @var resource odbc connection
     */
    protected $connection;

    protected $warnings = [];

    private $importedRowsCount = 0;

    private $timers = [];

    private $importedColumns = [];

    private $ignoreLines = 0;

    private $incremental = false;

    private $schemaName;

    const TIMESTAMP_COLUMN_NAME = '_timestamp';

    public function __construct($connection, $schemaName)
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
//                $this->dedup($stagingTableName, $columns, $this->getTablePrimaryKey($tableName));
                $this->addTimer('dedup', Debugger::timer('dedup'));
                $this->insertAllIntoTargetTable($stagingTableName, $tableName, $columns);
            }
            $this->dropTable($stagingTableName);
            $this->importedColumns = $columns;

            return new Result([
                'warnings' => $this->warnings,
                'timers' => $this->timers,
                'importedRowsCount' => $this->importedRowsCount,
                'importedColumns' => $this->importedColumns,
            ]);

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

        $moreColumns = array_diff($columnsToImport, $tableColumns);
        if (!empty($moreColumns)) {
            throw new Exception('Columns doest not match', Exception::COLUMNS_COUNT_NOT_MATCH);
        }
    }

    private function insertAllIntoTargetTable($stagingTableName, $targetTableName, $columns)
    {
        $this->query('BEGIN TRANSACTION');

        $targetTableNameWithSchema = $this->nameWithSchemaEscaped($targetTableName);
        $stagingTableNameWithSchema = $this->nameWithSchemaEscaped($stagingTableName);

        $this->query('TRUNCATE TABLE ' . $targetTableNameWithSchema);

        $columnsSql = implode(', ', array_map(function ($column) {
            return $this->quoteIdentifier($column);
        }, $columns));

        $now = $this->getNowFormatted();
        if (in_array(self::TIMESTAMP_COLUMN_NAME, $columns)) {
            $sql = "INSERT INTO {$targetTableNameWithSchema} ($columnsSql) (SELECT $columnsSql FROM $stagingTableNameWithSchema)";
        } else {
            $sql = "INSERT INTO {$targetTableNameWithSchema} ($columnsSql, \"" . self::TIMESTAMP_COLUMN_NAME . "\") (SELECT $columnsSql, '{$now}' FROM $stagingTableNameWithSchema)";
        }

        Debugger::timer('copyFromStagingToTarget');
        $this->query($sql);
        $this->addTimer('copyFromStagingToTarget', Debugger::timer('copyFromStagingToTarget'));

        $this->query('COMMIT');
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
            $columnsComparsionSql = array_map(function ($columnName) use ($targetTableNameWithSchema, $stagingTableNameWithSchema) {
                return sprintf(
                    "%s.%s != %s.%s",
                    $targetTableNameWithSchema,
                    $this->quoteIdentifier($columnName),
                    $stagingTableNameWithSchema,
                    $this->quoteIdentifier($columnName)
                );
            }, $columns);
            $sql .= " AND (" . implode(' OR ', $columnsComparsionSql) . ") ";

            Debugger::timer('updateTargetTable');
            $this->query($sql);
            $this->addTimer('updateTargetTable', Debugger::timer('updateTargetTable'));

            // Delete updated rows from staging table
            $sql = "DELETE FROM " . $stagingTableNameWithSchema . " ";
            $sql .= "USING " . $targetTableNameWithSchema . " ";
            $sql .= "WHERE " . implode(' AND ', $pkWhereSql);

            Debugger::timer('deleteUpdatedRowsFromStaging');
            $this->query($sql);
            $this->addTimer('deleteUpdatedRowsFromStaging', Debugger::timer('deleteUpdatedRowsFromStaging'));

            // Dedup staging table
            Debugger::timer('dedupStaging');
            $this->dedup($stagingTableName, $columns, $primaryKey);
            $this->addTimer('dedupStaging', Debugger::timer('dedupStaging'));
        }

        // Insert from staging to target table
        $sql = "INSERT INTO " . $targetTableNameWithSchema . " (" . implode(', ', array_map(function ($column) {
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

    private function replaceTables($sourceTableName, $targetTableName)
    {
        $this->dropTable($targetTableName);
        $this->query("ALTER TABLE {$this->nameWithSchemaEscaped($sourceTableName)} RENAME TO {$this->quoteIdentifier($targetTableName)}");
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

        $pkSql = implode(',', array_map(function ($column) {
            return $this->quoteIdentifier($column);
        }, $primaryKey));

        $sql = "SELECT ";

        $sql .= implode(",", array_map(function ($column) {
            return "a." . $this->quoteIdentifier($column);
        }, $columns));

        $sql .= sprintf(" FROM (SELECT %s, ROW_NUMBER() OVER (PARTITION BY %s ORDER BY %s) AS \"row_number\" FROM %s)",
            implode(",", array_map(function ($column) {
                return $this->quoteIdentifier($column);
            }, $columns)),
            $pkSql,
            $pkSql,
            $this->nameWithSchemaEscaped($tableName)
        );

        $sql .= " AS a WHERE a.\"row_number\" = 1";
        $columnsSql = implode(', ', array_map(function ($column) {
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
            'CREATE TEMPORARY TABLE %s LIKE %s',
            $this->nameWithSchemaEscaped($tempName),
            $this->nameWithSchemaEscaped($sourceTableName)
        ));

        $this->query(sprintf(
            'ALTER TABLE %s DROP COLUMN %s',
            $this->nameWithSchemaEscaped($tempName),
            $this->quoteIdentifier(self::TIMESTAMP_COLUMN_NAME)
        ));

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

        return array_map(function ($row) {
            return $row['attname'];
        }, $this->connection->query($sql)->fetchAll());
    }

    private function getTableColumns($tableName)
    {
        return array_map(function ($column) {
            return $column['column_name'];
        }, $this->describeTable($tableName, $this->schemaName));
    }


    protected function query($sql, $bind = [])
    {
        $stmt = odbc_prepare($this->connection, $sql);
        odbc_execute($stmt, $bind);
        odbc_free_result($stmt);
    }

    protected function queryFetchAll($sql, $bind = [])
    {
        $stmt = odbc_prepare($this->connection, $sql);
        odbc_execute($stmt, $bind);
        $rows = [];
        while ($row = odbc_fetch_array($stmt)) {
            $rows[] = $row;
        }
        odbc_free_result($stmt);
        return $rows;
    }

    /**
     * @return string
     */
    private function getNowFormatted()
    {
        $currentDate = new \DateTime('now', new \DateTimeZone('UTC'));
        return $currentDate->format('Y-m-d H:i:s');
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
        $this->incremental = (bool)$incremental;
        return $this;
    }

    /**
     * @return int
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
        $this->ignoreLines = (int)$linesCount;
        return $this;
    }

    protected function addTimer($name, $value)
    {
        $this->timers[] = [
            'name' => $name,
            'durationSeconds' => $value,
        ];
    }

    protected function describeTable($tableName, $schemaName)
    {
        $res = odbc_exec($this->connection, sprintf('SHOW COLUMNS IN %s.%s', $this->quoteIdentifier($schemaName), $this->quoteIdentifier($tableName)));
        $columns = [];
        while ($row = odbc_fetch_array($res)) {
            $columns[] = $row;
        }
        odbc_free_result($res);
        return $columns;
    }

    protected function quoteIdentifier($value)
    {
        $q = '"';
        return ($q . str_replace("$q", "$q$q", $value) . $q);
    }
}
