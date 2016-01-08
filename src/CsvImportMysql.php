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
 *     Example:
 *   $import->setStaticValues(array('storageApiTransaction' => 45646513))
 *        ->import('orders', $csvFile);
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

class CsvImportMysql implements ImportInterface
{
    /**
     * @var \PDO
     */
    protected $_connection;

    protected $_columnNameFilter;

    protected $_warnings = array();

    protected $_timers = array();

    protected $_importedRowsCount = 0;

    protected $_importedColumns = array();

    protected $_incremental = false;

    protected $_ignoreLines = 0;

    public function __construct(\PDO $connection)
    {
        $this->_connection = $connection;
    }

    /**
     * @param $tableName
     * @param $columns
     * @param @CsvFile[] $sourceData
     */
    public function import($tableName, $columns, array $sourceData)
    {
        $this->_importedRowsCount = 0;
        $this->_importedColumns = 0;
        $this->_warnings = array();
        $this->_timers = array();

        $this->_validate($tableName, $columns);

        $stagingTableName = $this->_createStagingTable($tableName, $this->getIncremental());
        foreach ($sourceData as $csvFile) {
            $this->_importTableColumnsAll($stagingTableName, $columns, $csvFile);
        }

        if ($this->getIncremental()) {
            $importColumns = array_uintersect($this->_tableColumns($tableName), $columns, 'strcasecmp');
            $this->_insertOrUpdateTargetTable($stagingTableName, $tableName, $importColumns);
        } else {
            $this->_swapTables($stagingTableName, $tableName);
        }
        $this->_dropTable($stagingTableName, $this->getIncremental());

        return new Result(array(
            'warnings' => $this->_warnings,
            'timers' => $this->_timers,
            'importedRowsCount' => $this->_importedRowsCount,
            'importedColumns' => $this->_importedColumns,
        ));
    }

    protected function _createStagingTable($tableName, $temporary = false)
    {
        $tempName = '__temp_' . $this->_uniqueValue();
        if (!$temporary) {
            $this->_query('DROP TABLE IF EXISTS ' . $tempName);
        }
        $this->_query(sprintf(
            'CREATE %s TABLE %s LIKE %s',
            $temporary ? 'TEMPORARY' : '',
            $tempName,
            $tableName
        ));
        return $tempName;
    }

    private function _uniqueValue()
    {
        return str_replace('.', '_', uniqid('csvImport', true));
    }

    protected function _importTableColumnsAll($tableName, $columns, CsvFile $csvFile)
    {
        $importColumns = $this->_tableColumns($tableName);

        $loadColumnsOrdered = array();
        foreach ($columns as $columnName) {
            if (in_array(strtolower($columnName), array_map('strtolower', $importColumns))) {
                $loadColumnsOrdered[] = $columnName;
            } else {
                $loadColumnsOrdered[] = '@dummy'; // skip column
            }
        }

        $sql = '
			LOAD DATA LOCAL INFILE ' . $this->_connection->quote($csvFile) . '
			REPLACE INTO TABLE ' . $tableName . '
			FIELDS TERMINATED BY ' . $this->_connection->quote($csvFile->getDelimiter()) . '
			OPTIONALLY ENCLOSED BY ' . $this->_connection->quote($csvFile->getEnclosure()) . '
			ESCAPED BY ' . $this->_connection->quote($csvFile->getEscapedBy()) . '
			LINES TERMINATED BY ' . $this->_connection->quote($csvFile->getLineBreak()) . '
			IGNORE ' . (int)$this->getIgnoreLines() . ' LINES
			(' . implode(', ', $loadColumnsOrdered) . ')
		';

        Debugger::timer('csvImport.loadData');
        $stmt = $this->_query($sql);

        $basename = basename($csvFile->getRealPath());
        $this->_addTimer('loadData.' . $basename, Debugger::timer('csvImport.loadData'));

        $this->_importedColumns = $importColumns;
        $warnings = $this->_connection->query('SHOW WARNINGS')->fetchAll();
        if (!empty($warnings)) {
            $this->_warnings[$basename] = $warnings;
        }

        Debugger::timer('csvImport.importedRowsCount');
        $this->_importedRowsCount = $stmt->rowCount();
        $this->_addTimer('importedRowsCount', Debugger::timer('csvImport.importedRowsCount'));
    }

    protected function _insertOrUpdateTargetTable($sourceTable, $targetTable, $importColumns)
    {
        Debugger::timer('csvImport.insertIntoTargetTable');

        $connection = $this->_connection;

        $columnsListEscaped = function ($columns, $prefix = null) use ($connection) {
            return implode(', ', array_map(function ($columnName) use ($connection, $prefix) {
                return ($prefix ? $prefix . '.' : '') . $columnName;
            }, $columns));
        };

        $sql = 'INSERT INTO ' . $targetTable . ' (';
        $sql .= $columnsListEscaped($importColumns);
        $sql .= ') ';


        $sql .= 'SELECT ' . $columnsListEscaped($importColumns, 't') . ' FROM ' . $sourceTable . ' t ';
        $sql .= 'ON DUPLICATE KEY UPDATE ';

        $sql .= implode(', ', array_map(function ($columnName) use ($connection) {
            return $columnName . ' = t.' . $columnName;
        }, $importColumns));

        $this->_query($sql);

        $this->_addTimer('insertIntoTargetTable', Debugger::timer('csvImport.insertIntoTargetTable'));

    }

    protected function _swapTables($table1, $table2)
    {
        $tmpNameQuoted = $this->_uniqueValue();
        $this->_query("
			RENAME TABLE $table1 TO $tmpNameQuoted,
				$table2 TO $table1,
				$tmpNameQuoted TO $table2
		");
    }

    protected function _dropTable($tableName, $temporary = false)
    {
        $this->_query(sprintf('DROP %S TABLE %s',
            $temporary ? 'TEMPORARY' : '',
            $tableName
        ));
    }

    /**
     * @param $query
     * @return \Zend_Db_Statement_Pdo
     * @throws Exception
     */
    protected function _query($query)
    {
        try {
            $stmt = $this->_connection->prepare($query);
            $stmt->execute();
            return $stmt;
        } catch (\Exception $e) {
            throw $this->_convertException($e);
        }
    }

    protected function _validate($tableName, array $columns)
    {
        if (!$this->tableExists($tableName)) {
            throw new Exception(sprintf('Table %s not exists', $tableName), Exception::TABLE_NOT_EXISTS);
        }

        if (count($columns) == 0) {
            throw new Exception('No columns found in CSV file.', Exception::NO_COLUMNS);
        }

        $duplicates = self::duplicates($columns, false); // case insensitive search
        if (!empty($duplicates)) {
            throw new Exception('There are duplicate columns in CSV file: ' . implode(', ', $duplicates), Exception::DUPLICATE_COLUMN_NAMES);
        }
    }

    private function tableExists($tableName)
    {
        $statement = $this->_connection->prepare('SHOW TABLES LIKE ?');
        $statement->execute([$tableName]);

        return !!$statement->fetch();
    }

    protected function _tableColumns($tableName)
    {
        return array_keys($this->describeTable($tableName));
    }

    /**
     * @param \Exception $e
     * @return Exception
     */
    protected function _convertException(\Exception $e)
    {
        $code = 0;
        $message = $e->getMessage();
        if (strpos($e->getMessage(), 'SQLSTATE[42S02]') !== FALSE) {
            $code = Exception::TABLE_NOT_EXISTS;
            $message = 'Table not exists';
        }

        return new Exception($message, $code, $e);
    }

    /**
     * @return bool
     */
    public function getIncremental()
    {
        return $this->_incremental;
    }

    /**
     * @param $incremental
     * @return CsvImportMysql
     */
    public function setIncremental($incremental)
    {
        $this->_incremental = (bool)$incremental;
        return $this;
    }

    /**
     * @return boolean
     */
    public function getIgnoreLines()
    {
        return $this->_ignoreLines;
    }

    /**
     * @param $linesCount
     * @return $this
     */
    public function setIgnoreLines($linesCount)
    {
        $this->_ignoreLines = (int)$linesCount;
        return $this;
    }

    private function _addTimer($name, $value)
    {
        $this->_timers[] = array(
            'name' => $name,
            'durationSeconds' => $value,
        );
    }

    private static function duplicates(array $array, $caseSensitive = true)
    {
        if (!$caseSensitive) {
            $array = array_map('strtolower', $array);
        }
        return array_values(array_unique(array_diff_key($array, array_unique($array))));
    }

    private function quoteIdentifier($value)
    {
        $q = '"';
        return ($q . str_replace("$q", "$q$q", $value) . $q);
    }

    private function describeTable($tableName)
    {
        $sql = 'DESCRIBE ' . $tableName;
        $stmt = $this->_connection->query($sql);

        // Use FETCH_NUM so we are not dependent on the CASE attribute of the PDO connection
        $result = $stmt->fetchAll();

        $field = 0;
        $type = 1;
        $null = 2;
        $key = 3;
        $default = 4;
        $extra = 5;

        $desc = array();
        $i = 1;
        $p = 1;
        foreach ($result as $row) {
            list($length, $scale, $precision, $unsigned, $primary, $primaryPosition, $identity)
                = array(null, null, null, null, false, null, false);
            if (preg_match('/unsigned/', $row[$type])) {
                $unsigned = true;
            }
            if (preg_match('/^((?:var)?char)\((\d+)\)/', $row[$type], $matches)) {
                $row[$type] = $matches[1];
                $length = $matches[2];
            } else if (preg_match('/^decimal\((\d+),(\d+)\)/', $row[$type], $matches)) {
                $row[$type] = 'decimal';
                $precision = $matches[1];
                $scale = $matches[2];
            } else if (preg_match('/^float\((\d+),(\d+)\)/', $row[$type], $matches)) {
                $row[$type] = 'float';
                $precision = $matches[1];
                $scale = $matches[2];
            } else if (preg_match('/^((?:big|medium|small|tiny)?int)\((\d+)\)/', $row[$type], $matches)) {
                $row[$type] = $matches[1];
                // The optional argument of a MySQL int type is not precision
                // or length; it is only a hint for display width.
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
            $desc[$row[$field]] = array(
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
                'IDENTITY' => $identity
            );
            ++$i;
        }
        return $desc;
    }


}
