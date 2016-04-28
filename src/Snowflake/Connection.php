<?php
/**
 * Created by PhpStorm.
 * User: martinhalamicek
 * Date: 28/04/16
 * Time: 09:31
 */
namespace Keboola\Db\Import\Snowflake;

use Keboola\Db\Import\Exception;

class Connection
{
    /**
     * @var resource odbc handle
     */
    private  $connection;

    public function __construct(array $options)
    {
        $requiredOptions = [
            'host',
            'port',
            'database',
            'warehouse',
            'user',
            'password',
        ];

        $missingOptions = array_diff($requiredOptions, array_keys($options));
        if (!empty($missingOptions)) {
            throw new Exception('Missing options: ' . implode(', ', $missingOptions));
        }

        $dsn = "Driver=SnowflakeDSIIDriver;Server=" . $options['host'];
        $dsn .= ";Port=" . $options['port'];
        $dsn .= ";database=" . $options['database'];
        $dsn .= ";Warehouse=" . $options['warehouse'];
        $dsn .= ";Tracing=4";
        $dsn .= ";Query_Timeout=60";
        $connection = odbc_connect($dsn, $options['user'], $options['password']);
        try {
            odbc_exec($connection, "USE DATABASE " . $options['database']);
            odbc_exec($connection, "USE WAREHOUSE " . $options['warehouse']);
        } catch (\Exception $e) {
            throw new Exception("Initializing Snowflake connection failed: " . $e->getMessage(), null, $e);
        }
        $this->connection = $connection;
    }

    public function quoteIdentifier($value)
    {
        $q = '"';
        return ($q . str_replace("$q", "$q$q", $value) . $q);
    }

    public function describeTable($schemaName, $tableName)
    {
        return $this->fetchAll(sprintf('SHOW COLUMNS IN %s.%s', $this->quoteIdentifier($schemaName), $this->quoteIdentifier($tableName)));
    }

    public function getTableColumns($schemaName, $tableName)
    {
        return array_map(function ($column) {
            return $column['column_name'];
        }, $this->describeTable($schemaName, $tableName));
    }

    public function getTablePrimaryKey($schemaName, $tableName)
    {
        $cols = $this->fetchAll(sprintf("DESC TABLE %s.%s", $this->quoteIdentifier($schemaName), $this->quoteIdentifier($tableName)));
        $pkCols = [];
        foreach ($cols as $col) {
            if ($col['primary key'] !== 'Y') {
                continue;
            }
            $pkCols[] = $col['name'];
        }

        return $pkCols;
    }

    public function query($sql, array $bind = [])
    {
        $stmt = odbc_prepare($this->connection, $sql);
        odbc_execute($stmt, $bind);
        odbc_free_result($stmt);
    }

    public function fetchAll($sql, $bind = [])
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

}