<?php

namespace Keboola\Db\Import;

use Keboola\Csv\CsvFile;
use Tracy\Debugger;

abstract class RedshiftBase implements ImportInterface
{

    protected $connection;

    protected $warnings = [];

    private $importedRowsCount = 0;

    private $timers = [];

    private $importedColumns = [];

    private $ignoreLines = 0;

    private $incremental = false;

    private $schemaName;

    private $legacyFullImport = false;

    public function __construct(\PDO $connection, $schemaName, $legacyFullImport = false)
    {
        $this->connection = $connection;
        $this->schemaName = $schemaName;
        $this->legacyFullImport = (bool) $legacyFullImport;
    }

    /**
     * @param $tableName
     * @param $columns
     * @param array CsvFile $csvFiles
     * @param array $options
     *  - useTimestamp - update and use timestamp column. default true
     *  - copyOptions - additional copy options for import command
     *  - convertEmptyValuesToNull - convert empty values to NULL
     * @return mixed
     */
    public function import($tableName, $columns, array $sourceData, array $options = [])
    {
        $this->validateColumns($tableName, $columns);
        $primaryKey = $this->getTablePrimaryKey($tableName);
        $stagingTableName = $this->createTemporaryTableFromSourceTable($tableName, $primaryKey, $this->schemaName);

        $this->importDataToStagingTable($stagingTableName, $columns, $sourceData, $options);

        if ($this->getIncremental()) {
            $this->insertOrUpdateTargetTable(
                $stagingTableName,
                $tableName,
                $primaryKey,
                $columns,
                isset($options['useTimestamp']) ? $options['useTimestamp'] : true,
                isset($options["convertEmptyValuesToNull"]) ? $options["convertEmptyValuesToNull"] : []
            );
        } else {
            Debugger::timer('dedup');
            $this->dedup($stagingTableName, $columns, $primaryKey);
            $this->addTimer('dedup', Debugger::timer('dedup'));
            if ($this->legacyFullImport) {
                $this->insertAllIntoTargetTableLegacy(
                    $stagingTableName,
                    $tableName,
                    $columns,
                    isset($options['useTimestamp']) ? $options['useTimestamp'] : true,
                    isset($options["convertEmptyValuesToNull"]) ? $options["convertEmptyValuesToNull"] : []
                );
            } else {
                $this->insertAllIntoTargetTable(
                    $stagingTableName,
                    $tableName,
                    $primaryKey,
                    $columns,
                    isset($options['useTimestamp']) ? $options['useTimestamp'] : true,
                    isset($options["convertEmptyValuesToNull"]) ? $options["convertEmptyValuesToNull"] : []
                );
            }
        }
        $this->dropTempTable($stagingTableName);
        $this->importedColumns = $columns;
        return new Result([
            'warnings' => $this->warnings,
            'timers' => $this->timers,
            'importedRowsCount' => $this->importedRowsCount,
            'importedColumns' => $this->importedColumns,
            'legacyFullImport' => $this->legacyFullImport,
        ]);
    }

    abstract protected function importDataToStagingTable($stagingTempTableName, $columns, $sourceData, array $options = []);

    private function validateColumns($tableName, $columnsToImport)
    {
        if (count($columnsToImport) == 0) {
            throw new Exception(
                'No columns found in CSV file.',
                Exception::NO_COLUMNS,
                null,
                'csvImport.noColumns'
            );
        }

        $tableColumns = $this->getTableColumns($tableName);
        $columnsToImport = array_map('strtolower', $columnsToImport);
        $moreColumns = array_diff($columnsToImport, $tableColumns);
        if (!empty($moreColumns)) {
            throw new Exception('Columns doest not match', Exception::COLUMNS_COUNT_NOT_MATCH);
        }
    }

    private function insertAllIntoTargetTable($stagingTempTableName, $targetTableName, $primaryKey, $columns, $useTimestamp = true, array $convertEmptyValuesToNull = [])
    {
        // create table same as target table
        $newTargetTableName = $this->createTableFromSourceTable($targetTableName, $primaryKey, $this->schemaName);

        // insert data to new table from staging table
        $columnsSql = implode(', ', array_map(function ($column) {
            return $this->quoteIdentifier($column);
        }, $columns));

        $columnsSelectSql = implode(', ', array_map(function ($column) use ($convertEmptyValuesToNull) {
            if (in_array($column, $convertEmptyValuesToNull)) {
                return "CASE {$this->quoteIdentifier($column)}::VARCHAR WHEN '' THEN NULL ELSE {$this->quoteIdentifier($column)} END";
            }
            return $this->quoteIdentifier($column);
        }, $columns));

        $now = $this->getNowFormatted();
        $newTargetTableNameWithSchema = $this->nameWithSchemaEscaped($newTargetTableName);
        $stagingTableNameEscaped = $this->tableNameEscaped($stagingTempTableName);
        if (in_array('_timestamp', $columns) || $useTimestamp === false) {
            $sql = "INSERT INTO {$newTargetTableNameWithSchema} ($columnsSql) (SELECT $columnsSelectSql FROM $stagingTableNameEscaped)";
        } else {
            $sql = "INSERT INTO {$newTargetTableNameWithSchema} ($columnsSql, _timestamp) (SELECT $columnsSelectSql, '{$now}' FROM $stagingTableNameEscaped)";
        }

        Debugger::timer('copyFromStagingToTarget');
        $this->query($sql);
        $this->addTimer('copyFromStagingToTarget', Debugger::timer('copyFromStagingToTarget'));

        // swap tables in transaction
        $this->connection->beginTransaction();
        try {
            $targetTableNameWithSchema = $this->nameWithSchemaEscaped($targetTableName);
            $this->query(sprintf('DROP TABLE %s', $targetTableNameWithSchema));
            $this->query(sprintf(
                "ALTER TABLE %s RENAME TO %s",
                $newTargetTableNameWithSchema,
                $this->tableNameEscaped($targetTableName)
            ));
            $this->connection->commit();
        } catch (\Exception $e) {
            $this->connection->rollBack();
            throw $e;
        }
    }

    /**
     * @param $stagingTempTableName
     * @param $targetTableName
     * @param $columns
     * @param bool $useTimestamp
     * @param array $convertEmptyValuesToNull
     */
    private function insertAllIntoTargetTableLegacy($stagingTempTableName, $targetTableName, $columns, $useTimestamp = true, array $convertEmptyValuesToNull = [])
    {
        $this->connection->beginTransaction();

        $targetTableNameWithSchema = $this->nameWithSchemaEscaped($targetTableName);
        $stagingTableNameEscaped = $this->tableNameEscaped($stagingTempTableName);

        $this->query('TRUNCATE TABLE ' . $targetTableNameWithSchema);

        $columnsSql = implode(', ', array_map(function ($column) {
            return $this->quoteIdentifier($column);
        }, $columns));

        $columnsSelectSql = implode(', ', array_map(function ($column) use ($convertEmptyValuesToNull) {
            if (in_array($column, $convertEmptyValuesToNull)) {
                return "CASE {$this->quoteIdentifier($column)}::VARCHAR WHEN '' THEN NULL ELSE {$this->quoteIdentifier($column)} END";
            }
            return $this->quoteIdentifier($column);
        }, $columns));

        $now = $this->getNowFormatted();
        if (in_array('_timestamp', $columns) || $useTimestamp === false) {
            $sql = "INSERT INTO {$targetTableNameWithSchema} ($columnsSql) (SELECT $columnsSelectSql FROM $stagingTableNameEscaped)";
        } else {
            $sql = "INSERT INTO {$targetTableNameWithSchema} ($columnsSql, _timestamp) (SELECT $columnsSelectSql, '{$now}' FROM $stagingTableNameEscaped)";
        }

        Debugger::timer('copyFromStagingToTarget');
        $this->query($sql);
        $this->addTimer('copyFromStagingToTarget', Debugger::timer('copyFromStagingToTarget'));

        $this->connection->commit();
    }

    /**
     * Performs merge operation according to http://docs.aws.amazon.com/redshift/latest/dg/merge-specify-a-column-list.html
     * @param $stagingTempTableName
     * @param $targetTableName
     * @param array $primaryKey
     * @param $columns
     * @param bool $useTimestamp
     * @param array $convertEmptyValuesToNull
     */
    private function insertOrUpdateTargetTable(
        $stagingTempTableName,
        $targetTableName,
        array $primaryKey,
        $columns,
        $useTimestamp = true,
        array $convertEmptyValuesToNull = []
    ) {
        $this->connection->beginTransaction();
        $nowFormatted = $this->getNowFormatted();

        $targetTableNameWithSchema = $this->nameWithSchemaEscaped($targetTableName);
        $stagingTableNameEscaped = $this->tableNameEscaped($stagingTempTableName);

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

            $sql .= implode(', ', $columnsSet);
            if ($useTimestamp) {
                $sql .= ", _timestamp = '{$nowFormatted}' ";
            }
            $sql .= " FROM " . $stagingTableNameEscaped . " ";
            $sql .= " WHERE ";

            $pkWhereSql = [];
            foreach ($primaryKey as $pkColumn) {
                $pkWhereSql[] = sprintf(
                    "%s.%s = %s.%s",
                    $targetTableNameWithSchema,
                    $this->quoteIdentifier($pkColumn),
                    $stagingTableNameEscaped,
                    $this->quoteIdentifier($pkColumn)
                );
            }

            $sql .= implode(' AND ', $pkWhereSql) . " ";

            // update only changed rows - mysql TIMESTAMP ON UPDATE behaviour simulation
            $columnsComparsionSql = array_map(function ($columnName) use (
                $targetTableNameWithSchema,
                $stagingTableNameEscaped
            ) {
                return sprintf(
                    "COALESCE(CAST(%s.%s as varchar), '') != COALESCE(CAST(%s.%s as varchar), '')",
                    $targetTableNameWithSchema,
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
            $sql = "DELETE FROM " . $stagingTableNameEscaped . " ";
            $sql .= "USING " . $targetTableNameWithSchema . " ";
            $sql .= "WHERE " . implode(' AND ', $pkWhereSql);

            Debugger::timer('deleteUpdatedRowsFromStaging');
            $this->query($sql);
            $this->addTimer('deleteUpdatedRowsFromStaging', Debugger::timer('deleteUpdatedRowsFromStaging'));

            // Dedup staging table
            Debugger::timer('dedupStaging');
            $this->dedup($stagingTempTableName, $columns, $primaryKey);
            $this->addTimer('dedupStaging', Debugger::timer('dedupStaging'));
        }

        // Insert from staging to target table
        $sql = "INSERT INTO " . $targetTableNameWithSchema . " (" . implode(', ', array_map(function ($column) use ($convertEmptyValuesToNull) {
            return $this->quoteIdentifier($column);
        }, $columns));

        $sql .= ($useTimestamp) ? ", _timestamp) " : ")";

        $columnsSetSql = [];

        foreach ($columns as $columnName) {
            if (in_array($columnName, $convertEmptyValuesToNull)) {
                $columnsSetSql[] = sprintf(
                    "CASE %s.%s::VARCHAR WHEN '' THEN NULL ELSE %s.%s END",
                    $stagingTableNameEscaped,
                    $this->quoteIdentifier($columnName),
                    $stagingTableNameEscaped,
                    $this->quoteIdentifier($columnName)
                );
            } else {
                $columnsSetSql[] = sprintf(
                    "%s.%s",
                    $stagingTableNameEscaped,
                    $this->quoteIdentifier($columnName)
                );
            }
        }
        $sql .= "SELECT " . implode(', ', $columnsSetSql);
        $sql .= ($useTimestamp) ? ", '{$nowFormatted}'" : "";
        $sql .= " FROM " . $stagingTableNameEscaped;
        Debugger::timer('insertIntoTargetFromStaging');
        $this->query($sql);
        $this->addTimer('insertIntoTargetFromStaging', Debugger::timer('insertIntoTargetFromStaging'));

        $this->connection->commit();
    }

    private function replaceTempTables($sourceTableName, $targetTableName)
    {
        $this->dropTempTable($targetTableName);
        $this->query("ALTER TABLE {$this->tableNameEscaped($sourceTableName)} RENAME TO {$this->tableNameEscaped($targetTableName)}");
    }

    private function dropTempTable($tableName)
    {
        $this->query("DROP TABLE " . $this->tableNameEscaped($tableName));
    }

    protected function nameWithSchemaEscaped($tableName, $schemaName = null)
    {
        if ($schemaName === null) {
            $schemaName = $this->schemaName;
        }
        return "\"{$schemaName}\"." . $this->tableNameEscaped($tableName);
    }

    protected function tableNameEscaped($tableName)
    {
        $tableNameFiltered = preg_replace('/[^a-zA-Z0-9_\-\.]+/', "", $tableName);
        return "\"{$tableNameFiltered}\"";
    }

    private function uniqueValue()
    {
        return str_replace('.', '_', uniqid('csvimport', true));
    }

    private function dedup($inputTempTableName, $columns, array $primaryKey)
    {
        if (empty($primaryKey)) {
            return;
        }

        $pkSql = implode(', ', array_map(function ($column) {
            return $this->quoteIdentifier($column);
        }, $primaryKey));

        $sql = "SELECT ";

        $sql .= implode(", ", array_map(function ($column) {
            return "a." . $this->quoteIdentifier($column);
        }, $columns));

        $sql .= sprintf(
            " FROM (SELECT %s, ROW_NUMBER() OVER (PARTITION BY %s ORDER BY %s) AS \"_row_number_\" FROM %s)",
            implode(", ", array_map(function ($column) {
                return $this->quoteIdentifier($column);
            }, $columns)),
            $pkSql,
            $pkSql,
            $this->tableNameEscaped($inputTempTableName)
        );

        $sql .= " AS a WHERE a.\"_row_number_\" = 1";
        $columnsSql = implode(', ', array_map(function ($column) {
            return $this->quoteIdentifier($column);
        }, $columns));

        $tempTable = $this->createTemporaryTableFromSourceTable($inputTempTableName, $primaryKey);

        $this->query("INSERT INTO {$this->tableNameEscaped($tempTable)} ($columnsSql) " . $sql);
        $this->replaceTempTables($tempTable, $inputTempTableName);
    }

    private function createTemporaryTableFromSourceTable($sourceTableName, array $primaryKey, $schemaName = null)
    {
        $tempName = '__temp_' . $this->uniqueValue();
        $this->query(sprintf(
            'CREATE TEMPORARY TABLE %s (LIKE %s)',
            $this->tableNameEscaped($tempName),
            $schemaName ? $this->nameWithSchemaEscaped($sourceTableName, $schemaName) : $this->tableNameEscaped($sourceTableName)
        ));

        // PK is not copied - add it to the table
        $tableIdentifier = $this->tableNameEscaped($tempName);
        if (!empty($primaryKey)) {
            $this->query(sprintf(
                "
                    ALTER TABLE %s
                    ADD PRIMARY KEY (%s)
                ",
                $tableIdentifier,
                implode(', ', array_map(function ($columnName) {
                    return $this->quoteIdentifier($columnName);
                }, $primaryKey))
            ));
        }

        return $tempName;
    }

    private function createTableFromSourceTable($sourceTableName, array $primaryKey, $schemaName)
    {
        $tempName = '__temp_' . $this->uniqueValue();
        $this->query(sprintf(
            'CREATE TABLE %s (LIKE %s)',
            $this->nameWithSchemaEscaped($tempName, $schemaName),
            $this->nameWithSchemaEscaped($sourceTableName, $schemaName)
        ));

        // PK is not copied - add it to the table
        $tableIdentifier = $this->nameWithSchemaEscaped($tempName, $schemaName);
        if (!empty($primaryKey)) {
            $this->query(sprintf(
                "
                    ALTER TABLE %s
                    ADD PRIMARY KEY (%s)
                ",
                $tableIdentifier,
                implode(', ', array_map(function ($columnName) {
                    return $this->quoteIdentifier($columnName);
                }, $primaryKey))
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

        return array_map(function ($row) {
            return $row['attname'];
        }, $this->queryFetchAll($sql));
    }

    private function getTableColumns($tableName)
    {
        return array_map(
            'strtolower',
            array_keys($this->describeTable(strtolower($tableName), strtolower($this->schemaName)))
        );
    }

    protected function query($sql, $bind = [])
    {
        try {
            $this->connection->prepare($sql)->execute($bind);
        } catch (\PDOException $e) {
            throw $this->handleQueryException($e);
        }
    }

    private function queryFetchAll($sql)
    {
        try {
            return $this->connection->query($sql)->fetchAll();
        } catch (\PDOException $e) {
            throw $this->handleQueryException($e);
        }
    }

    private function handleQueryException(\PDOException $e)
    {
        if (strpos($e->getMessage(), 'Mandatory url is not present in manifest file') !== false) {
            return new Exception('Mandatory url is not present in manifest file', Exception::MANDATORY_FILE_NOT_FOUND);
        }

        if (strpos($e->getMessage(), 'SQLSTATE[57014]') !== false) {
            return new Exception('Statement timeout. Maximum query execution time exeeded.', Exception::QUERY_TIMEOUT);
        }

        return $e;
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
        $this->incremental = (bool)$incremental;
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
            WHERE a.attnum > 0 AND c.relname = " . $this->connection->quote($tableName);
        if ($schemaName) {
            $sql .= " AND n.nspname = " . $this->connection->quote($schemaName);
        }
        $sql .= ' ORDER BY a.attnum';

        $result = $this->queryFetchAll($sql);

        $attnum = 0;
        $nspname = 1;
        $relname = 2;
        $colname = 3;
        $type = 4;
        $atttypemod = 5;
        $complete_type = 6;
        $default_value = 7;
        $notnull = 8;
        $length = 9;
        $contype = 10;
        $conkey = 11;

        $desc = [];
        foreach ($result as $key => $row) {
            $defaultValue = $row[$default_value];
            if ($row[$type] == 'varchar' || $row[$type] == 'bpchar') {
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
            list($primary, $primaryPosition, $identity) = [false, null, false];
            if ($row[$contype] == 'p') {
                $primary = true;
                $primaryPosition = array_search($row[$attnum], explode(',', $row[$conkey])) + 1;
                $identity = (bool)(preg_match('/^nextval/', $row[$default_value]));
            }
            $desc[$row[$colname]] = [
                'SCHEMA_NAME' => $row[$nspname],
                'TABLE_NAME' => $row[$relname],
                'COLUMN_NAME' => $row[$colname],
                'COLUMN_POSITION' => $row[$attnum],
                'DATA_TYPE' => $row[$type],
                'DEFAULT' => $defaultValue,
                'NULLABLE' => (bool)($row[$notnull] != 't'),
                'LENGTH' => $row[$length],
                'SCALE' => null, // @todo
                'PRECISION' => null, // @todo
                'UNSIGNED' => null, // @todo
                'PRIMARY' => $primary,
                'PRIMARY_POSITION' => $primaryPosition,
                'IDENTITY' => $identity,
            ];
        }
        return $desc;
    }

    protected function quoteIdentifier($value)
    {
        $q = '"';
        return ($q . str_replace("$q", "$q$q", $value) . $q);
    }
}
