<?php

namespace Keboola\Db\Import;

use Keboola\Csv\CsvFile;
use Tracy\Debugger;

class CsvImportMysql implements ImportInterface
{
    /**
     * @var \PDO
     */
    protected $connection;

    protected $columnNameFilter;

    protected $warnings = [];

    protected $timers = [];

    protected $importedRowsCount = 0;

    protected $importedColumns = [];

    protected $incremental = false;

    protected $ignoreLines = 0;

    public function __construct(\PDO $connection)
    {
        $this->connection = $connection;
    }

    /**
     * @param $tableName
     * @param $columns
     * @param CsvFile[] $sourceData
     * @param array $options
     * @return Result
     */
    public function import($tableName, $columns, array $sourceData, array $options = [])
    {
        $this->importedRowsCount = 0;
        $this->importedColumns = 0;
        $this->warnings = [];
        $this->timers = [];

        $this->validate($tableName, $columns);

        $stagingTableName = $this->createStagingTable($tableName, $this->getIncremental());
        foreach ($sourceData as $csvFile) {
            $this->importTableColumnsAll(
                $stagingTableName,
                $columns,
                $csvFile,
                isset($options["convertEmptyValuesToNull"]) ? $options["convertEmptyValuesToNull"] : []
            );
        }

        if ($this->getIncremental()) {
            $importColumns = array_uintersect($this->tableColumns($tableName), $columns, 'strcasecmp');
            $this->insertOrUpdateTargetTable(
                $stagingTableName,
                $tableName,
                $importColumns,
                isset($options["convertEmptyValuesToNull"]) ? $options["convertEmptyValuesToNull"] : []
            );
        } else {
            $this->swapTables($stagingTableName, $tableName);
        }
        $this->dropTable($stagingTableName, $this->getIncremental());

        return new Result([
            'warnings' => $this->warnings,
            'timers' => $this->timers,
            'importedRowsCount' => $this->importedRowsCount,
            'importedColumns' => $this->importedColumns,
        ]);
    }

    protected function createStagingTable($tableName, $temporary = false)
    {
        $tempName = '__temp_' . $this->uniqueValue();
        if (!$temporary) {
            $this->query('DROP TABLE IF EXISTS ' . $this->quoteIdentifier($tempName));
        }
        $this->query(sprintf(
            'CREATE %s TABLE %s LIKE %s',
            $temporary ? 'TEMPORARY' : '',
            $this->quoteIdentifier($tempName),
            $this->quoteIdentifier($tableName)
        ));
        return $tempName;
    }

    private function uniqueValue()
    {
        return str_replace('.', '_', uniqid('csvImport', true));
    }

    protected function importTableColumnsAll($tableName, $columns, CsvFile $csvFile, array $convertEmptyValuesToNull = [])
    {
        $importColumns = $this->tableColumns($tableName);

        $loadColumnsOrdered = [];
        foreach ($columns as $columnName) {
            if (in_array(strtolower($columnName), array_map('strtolower', $importColumns))) {
                $loadColumnsOrdered[] = $this->quoteIdentifier($columnName);
            } else {
                $loadColumnsOrdered[] = '@dummy'; // skip column
            }
        }

        $stagingTableName = $this->createStagingTable($tableName, true);

        $sql = '
			LOAD DATA LOCAL INFILE ' . $this->connection->quote($csvFile) . '
			REPLACE INTO TABLE ' . $this->quoteIdentifier($stagingTableName) . '
			CHARACTER SET UTF8
			FIELDS TERMINATED BY ' . $this->connection->quote($csvFile->getDelimiter()) . '
			OPTIONALLY ENCLOSED BY ' . $this->connection->quote($csvFile->getEnclosure()) . '
			ESCAPED BY ' . $this->connection->quote($csvFile->getEscapedBy()) . '
			LINES TERMINATED BY ' . $this->connection->quote($csvFile->getLineBreak()) . '
			IGNORE ' . (int)$this->getIgnoreLines() . ' LINES
			(' . implode(', ', $loadColumnsOrdered) . ')
		';

        Debugger::timer('csvImport.loadData');
        $stmt = $this->query($sql);

        $basename = basename($csvFile->getRealPath());
        $this->addTimer('loadData.' . $basename, Debugger::timer('csvImport.loadData'));

        $this->importedColumns = $importColumns;
        $warnings = $this->connection->query('SHOW WARNINGS')->fetchAll();
        if (!empty($warnings)) {
            $this->warnings[$basename] = $warnings;
        }

        Debugger::timer('csvImport.importedRowsCount');
        $this->importedRowsCount = $stmt->rowCount();
        $this->addTimer('importedRowsCount', Debugger::timer('csvImport.importedRowsCount'));


        $columnsListEscaped = function ($columns) {
            return implode(', ', array_map(function ($columnName) {
                return $this->quoteIdentifier($columnName);
            }, $columns));
        };

        $columnsListEscapedSelect = function ($columns, $prefix, $convertEmptyValuesToNull) {
            return implode(', ', array_map(function ($columnName) use ($prefix, $convertEmptyValuesToNull) {
                if (in_array($columnName, $convertEmptyValuesToNull)) {
                    $column = $prefix . '.' . $this->quoteIdentifier($columnName);
                    return "IF({$column} = '', NULL, {$column})";
                }
                return ($prefix ? $prefix . '.' : '') . $this->quoteIdentifier($columnName);
            }, $columns));
        };

        $updateDuplicateColumns = function ($columns, $prefix, $convertEmptyValuesToNull) {
            return implode(', ', array_map(function ($columnName) use ($prefix, $convertEmptyValuesToNull) {
                if (in_array($columnName, $convertEmptyValuesToNull)) {
                    return sprintf(
                        "%s = IF(%s.%s = '', NULL, %s.%s)",
                        $this->quoteIdentifier($columnName),
                        $prefix,
                        $this->quoteIdentifier($columnName),
                        $prefix,
                        $this->quoteIdentifier($columnName)
                    );
                }
                return $this->quoteIdentifier($columnName) . ' = ' . $prefix . '.' . $this->quoteIdentifier($columnName);
            }, $columns));
        };

        // convertEmptyValuesToNull
        $sql = 'INSERT INTO ' . $this->quoteIdentifier($tableName) . ' (';
        $sql .= $columnsListEscaped($importColumns);
        $sql .= ') ';
        $sql .= 'SELECT ' . $columnsListEscapedSelect($importColumns,
                't', $convertEmptyValuesToNull) . ' FROM ' . $this->quoteIdentifier($stagingTableName) . ' t ';
        $sql .= 'ON DUPLICATE KEY UPDATE ';
        $sql .= $updateDuplicateColumns($importColumns, 't', $convertEmptyValuesToNull);

        $this->query($sql);
    }

    protected function insertOrUpdateTargetTable($sourceTable, $targetTable, $importColumns, array $convertEmptyValuesToNull = [])
    {
        Debugger::timer('csvImport.insertIntoTargetTable');

        $connection = $this->connection;

        $columnsListEscaped = function ($columns, $prefix = null) {
            return implode(', ', array_map(function ($columnName) use ($prefix) {
                return ($prefix ? $prefix . '.' : '') . $this->quoteIdentifier($columnName);
            }, $columns));
        };

        $columnsListEscapedSelect = function ($columns, $prefix = null, $convertEmptyValuesToNull = []) {
            return implode(', ', array_map(function ($columnName) use ($prefix, $convertEmptyValuesToNull) {
                if (in_array($columnName, $convertEmptyValuesToNull)) {
                    $column = ($prefix ? $prefix . '.' : '') . $this->quoteIdentifier($columnName);
                    return "IF({$column} = '', NULL, {$column})";
                }
                return ($prefix ? $prefix . '.' : '') . $this->quoteIdentifier($columnName);
            }, $columns));
        };

        $sql = 'INSERT INTO ' . $this->quoteIdentifier($targetTable) . ' (';
        $sql .= $columnsListEscaped($importColumns);
        $sql .= ') ';

        $sql .= 'SELECT ' . $columnsListEscapedSelect($importColumns,
                't', $convertEmptyValuesToNull) . ' FROM ' . $this->quoteIdentifier($sourceTable) . ' t ';
        $sql .= 'ON DUPLICATE KEY UPDATE ';

        $sql .= implode(', ', array_map(function ($columnName) use ($connection) {
            return $this->quoteIdentifier($columnName) . ' = t.' . $this->quoteIdentifier($columnName);
        }, $importColumns));

        $this->query($sql);

        $this->addTimer('insertIntoTargetTable', Debugger::timer('csvImport.insertIntoTargetTable'));
    }

    protected function swapTables($table1, $table2)
    {
        $tmpNameQuoted = $this->quoteIdentifier($this->uniqueValue());
        $table1Quoted = $this->quoteIdentifier($table1);
        $table2Quoted = $this->quoteIdentifier($table2);
        $this->query("
            RENAME TABLE $table1Quoted TO $tmpNameQuoted,
                $table2Quoted TO $table1Quoted,
                $tmpNameQuoted TO $table2Quoted
        ");
    }

    protected function dropTable($tableName, $temporary = false)
    {
        $this->query(sprintf(
            'DROP %S TABLE %s',
            $temporary ? 'TEMPORARY' : '',
            $this->quoteIdentifier($tableName)
        ));
    }

    /**
     * @param $query
     * @return \Zend_Db_Statement_Pdo
     * @throws Exception
     */
    protected function query($query)
    {
        try {
            $stmt = $this->connection->prepare($query);
            $stmt->execute();
            return $stmt;
        } catch (\Exception $e) {
            throw $this->convertException($e);
        }
    }

    protected function validate($tableName, array $columns)
    {
        if (!$this->tableExists($tableName)) {
            throw new Exception(sprintf('Table %s not exists', $tableName), Exception::TABLE_NOT_EXISTS);
        }

        if (count($columns) == 0) {
            throw new Exception('No columns found in CSV file.', Exception::NO_COLUMNS);
        }

        $duplicates = self::duplicates($columns, false); // case insensitive search
        if (!empty($duplicates)) {
            throw new Exception(
                'There are duplicate columns in CSV file: ' . implode(', ', $duplicates),
                Exception::DUPLICATE_COLUMN_NAMES
            );
        }
    }

    private function tableExists($tableName)
    {
        $statement = $this->connection->prepare('SHOW TABLES LIKE ?');
        $statement->execute([$tableName]);

        return !!$statement->fetch();
    }

    protected function tableColumns($tableName)
    {
        return array_keys($this->describeTable($tableName));
    }

    /**
     * @param \Exception $e
     * @return Exception
     */
    protected function convertException(\Exception $e)
    {
        $code = 0;
        $message = $e->getMessage();
        if (strpos($e->getMessage(), 'SQLSTATE[42S02]') !== false) {
            $code = Exception::TABLE_NOT_EXISTS;
            $message = 'Table not exists';
        } else {
            if (strpos($e->getMessage(), '1118') !== false) {
                $code = Exception::ROW_SIZE_TOO_LARGE;
            }
        }
        return new Exception($message, $code, $e);
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

    private function addTimer($name, $value)
    {
        $this->timers[] = [
            'name' => $name,
            'durationSeconds' => $value,
        ];
    }

    private static function duplicates(array $array, $caseSensitive = true)
    {
        if (!$caseSensitive) {
            $array = array_map('strtolower', $array);
        }
        return array_values(array_unique(array_diff_key($array, array_unique($array))));
    }

    private function describeTable($tableName)
    {
        $sql = 'DESCRIBE ' . $this->quoteIdentifier($tableName);
        $stmt = $this->connection->query($sql);

        // Use FETCH_NUM so we are not dependent on the CASE attribute of the PDO connection
        $result = $stmt->fetchAll();

        $field = 0;
        $type = 1;
        $null = 2;
        $key = 3;
        $default = 4;
        $extra = 5;

        $desc = [];
        $i = 1;
        $p = 1;
        foreach ($result as $row) {
            list($length, $scale, $precision, $unsigned, $primary, $primaryPosition, $identity)
                = [null, null, null, null, false, null, false];
            if (preg_match('/unsigned/', $row[$type])) {
                $unsigned = true;
            }
            if (preg_match('/^((?:var)?char)\((\d+)\)/', $row[$type], $matches)) {
                $row[$type] = $matches[1];
                $length = $matches[2];
            } else {
                if (preg_match('/^decimal\((\d+),(\d+)\)/', $row[$type], $matches)) {
                    $row[$type] = 'decimal';
                    $precision = $matches[1];
                    $scale = $matches[2];
                } else {
                    if (preg_match('/^float\((\d+),(\d+)\)/', $row[$type], $matches)) {
                        $row[$type] = 'float';
                        $precision = $matches[1];
                        $scale = $matches[2];
                    } else {
                        if (preg_match('/^((?:big|medium|small|tiny)?int)\((\d+)\)/', $row[$type], $matches)) {
                            $row[$type] = $matches[1];
                            // The optional argument of a MySQL int type is not precision
                            // or length; it is only a hint for display width.
                        }
                    }
                }
            }
            if (strtoupper($row[$key]) == 'PRI') {
                $primary = true;
                $primaryPosition = $p;
                if ($row[$extra] == 'auto_increment') {
                    $identity = true;
                } else {
                    $identity = false;
                }
                ++$p;
            }
            $desc[$row[$field]] = [
                'SCHEMA_NAME' => null, // @todo
                'TABLE_NAME' => $tableName,
                'COLUMN_NAME' => $row[$field],
                'COLUMN_POSITION' => $i,
                'DATA_TYPE' => $row[$type],
                'DEFAULT' => $row[$default],
                'NULLABLE' => (bool)($row[$null] == 'YES'),
                'LENGTH' => $length,
                'SCALE' => $scale,
                'PRECISION' => $precision,
                'UNSIGNED' => $unsigned,
                'PRIMARY' => $primary,
                'PRIMARY_POSITION' => $primaryPosition,
                'IDENTITY' => $identity,
            ];
            ++$i;
        }
        return $desc;
    }

    private function quoteIdentifier($value)
    {
        $q = '`';
        return ($q . str_replace("$q", "$q$q", $value) . $q);
    }
}
