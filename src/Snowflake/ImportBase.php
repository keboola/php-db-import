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
        $stagingTableName = $this->createStagingTable($columns);

        try {
            $this->importDataToStagingTable($stagingTableName, $columns, $sourceData);

            if ($this->getIncremental()) {
                $this->insertOrUpdateTargetTable(
                    $stagingTableName,
                    $tableName,
                    $columns);
            } else {
                Debugger::timer('dedup');
                $this->dedupe($stagingTableName, $columns, $this->getTablePrimaryKey($tableName));
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
        $this->query('BEGIN TRANSACTION');
        $nowFormatted = $this->getNowFormatted();

        $targetTableNameWithSchema = $this->nameWithSchemaEscaped($targetTableName);
        $stagingTableNameWithSchema = $this->nameWithSchemaEscaped($stagingTableName);
        $targetTableNameEscaped = $this->quoteIdentifier($targetTableName);
        $stagingTableNameEscaped = $this->quoteIdentifier($stagingTableName);

        $primaryKey = $this->getTablePrimaryKey($targetTableName);

        if (!empty($primaryKey)) {

            // Update target table
            $sql = "UPDATE " . $targetTableNameWithSchema . " SET ";

            $columnsSet = [];
            foreach ($columns as $columnName) {
                $columnsSet[] = sprintf(
                    "%s = %s.%s",
                    $this->quoteIdentifier($columnName),
                    $stagingTableNameEscaped,
                    $this->quoteIdentifier($columnName)
                );
            }

            $sql .= implode(', ', $columnsSet) . ", " . $this->quoteIdentifier(self::TIMESTAMP_COLUMN_NAME) . " = '{$nowFormatted}' ";
            $sql .= " FROM " . $stagingTableNameWithSchema . " ";
            $sql .= " WHERE ";

            $pkWhereSql = [];
            foreach ($primaryKey as $pkColumn) {
                $pkWhereSql[] = sprintf(
                    "%s.%s = %s.%s",
                    $targetTableNameEscaped,
                    $this->quoteIdentifier($pkColumn),
                    $stagingTableNameEscaped,
                    $this->quoteIdentifier($pkColumn)
                );
            }

            $sql .= implode(' AND ', $pkWhereSql) . " ";

            // update only changed rows - mysql TIMESTAMP ON UPDATE behaviour simulation
            $columnsComparsionSql = array_map(function ($columnName) use ($targetTableNameEscaped, $stagingTableNameEscaped) {
                return sprintf(
                    "%s.%s != %s.%s",
                    $targetTableNameEscaped,
                    $this->quoteIdentifier($columnName),
                    $stagingTableNameEscaped,
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
            $this->dedupe($stagingTableName, $columns, $primaryKey);
            $this->addTimer('dedupStaging', Debugger::timer('dedupStaging'));
        }

        // Insert from staging to target table
        $sql = "INSERT INTO " . $targetTableNameWithSchema . " (" . implode(', ', array_map(function ($column) {
                return $this->quoteIdentifier($column);
            }, array_merge($columns, [self::TIMESTAMP_COLUMN_NAME]))) . ")";

        $columnsSetSql = [];

        foreach ($columns as $columnName) {
            $columnsSetSql[] = sprintf(
                "%s.%s",
                $stagingTableNameEscaped,
                $this->quoteIdentifier($columnName)
            );
        }

        $sql .= "SELECT " . implode(',', $columnsSetSql) . ", '{$nowFormatted}' ";
        $sql .= "FROM " . $stagingTableNameWithSchema;
        Debugger::timer('insertIntoTargetFromStaging');
        $this->query($sql);
        $this->addTimer('insertIntoTargetFromStaging', Debugger::timer('insertIntoTargetFromStaging'));

        $this->query('COMMIT');
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

    private function dedupe($tableName, $columns, array $primaryKey)
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

        $tempTable = $this->createStagingTable($columns);

        $this->query("INSERT INTO {$this->nameWithSchemaEscaped($tempTable)} ($columnsSql) " . $sql);
        $this->replaceTables($tempTable, $tableName);
    }

    private function createStagingTable(array $columns) {

        $tempName = '__temp_' . $this->uniqueValue();

        $columnsSql = array_map(function($column) {
            return sprintf('%s varchar', $this->quoteIdentifier($column));
        }, $columns);

        $this->query(sprintf(
            'CREATE TEMPORARY TABLE %s (%s)',
            $this->nameWithSchemaEscaped($tempName),
            implode(', ', $columnsSql)
        ));

        return $tempName;
    }

    /**
     * @param $tableName
     * @return array
     */
    private function getTablePrimaryKey($tableName)
    {
        $cols = $this->queryFetchAll(sprintf("DESC TABLE %s", $this->nameWithSchemaEscaped($tableName)));
        $pkCols = [];
        foreach ($cols as $col) {
            if ($col['primary key'] !== 'Y') {
                continue;
            }
            $pkCols[] = $col['name'];
        }

        return $pkCols;
    }

    private function getTableColumns($tableName)
    {
        return array_map(function ($column) {
            return $column['column_name'];
        }, $this->describeTable($tableName, $this->schemaName));
    }


    protected function query($sql, $bind = [])
    {
        echo $sql . "\n";
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
