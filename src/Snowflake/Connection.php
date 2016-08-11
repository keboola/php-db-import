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

    /**
     * The connection constructor accepts the following options:
     * - host (string, required) - hostname
     * - port (int, optional) - port - default 443
     * - user (string, required) - username
     * - password (string, required) - password
     * - warehouse (string) - default warehouse to use
     * - database (string) - default database to use
     * - tracing (int) - the level of detail to be logged in the driver trace files
     * - loginTimeout (int) - Specifies how long to wait for a response when connecting to the Snowflake service before returning a login failure error.
     * - networkTimeout (int) - Specifies how long to wait for a response when interacting with the Snowflake service before returning an error. Zero (0) indicates no network timeout is set.
     * - queryTimeout (int) - Specifies how long to wait for a query to complete before returning an error. Zero (0) indicates to wait indefinitely.
     *
     * @param array $options
     */
    public function __construct(array $options)
    {
        $requiredOptions = [
            'host',
            'user',
            'password',
        ];

        $missingOptions = array_diff($requiredOptions, array_keys($options));
        if (!empty($missingOptions)) {
            throw new Exception('Missing options: ' . implode(', ', $missingOptions));
        }

        $port = isset($options['port']) ? (int) $options['port'] : 443;
        $tracing = isset($options['tracing']) ? (int) $options['tracing'] : 0;

        $dsn = "Driver=SnowflakeDSIIDriver;Server=" . $options['host'];
        $dsn .= ";Port=" . $port;
        $dsn .= ";Tracing="  . $tracing;

        if (isset($options['loginTimeout'])) {
            $dsn .= ";Login_timeout=" . (int) $options['loginTimeout'];
        }

        if (isset($options['networkTimeout'])) {
            $dsn .= ";Network_timeout=" . (int) $options['networkTimeout'];
        }

        if (isset($options['queryTimeout'])) {
            $dsn .= ";Query_timeout=" . (int) $options['queryTimeout'];
        }

        if (isset($options['database'])) {
            $dsn .= ";Database=" . $this->quoteIdentifier($options['database']);
        }

        if (isset($options['warehouse'])) {
            $dsn .= ";Warehouse=" . $this->quoteIdentifier($options['warehouse']);
        }

        try {
            $this->connection = odbc_connect($dsn, $options['user'], $options['password']);
        } catch (\Exception $e) {
            throw new Exception("Initializing Snowflake connection failed: " . $e->getMessage(), null, $e);
        }
    }

    public function quoteIdentifier($value)
    {
        $q = '"';
        return ($q . str_replace("$q", "$q$q", $value) . $q);
    }

    /**
     * Returns information about table:
     *  - name
     *  - bytes
     *  - rows
     * @param $schemaName
     * @param $tableName
     * @return array
     * @throws Exception
     */
    public function describeTable($schemaName, $tableName)
    {
        $tables = $this->fetchAll(sprintf(
            "SHOW TABLES LIKE %s IN SCHEMA %s",
            "'" . addslashes($tableName) . "'",
            $this->quoteIdentifier($schemaName)
        ));

        foreach ($tables as $table) {
            if ($table['name'] === $tableName) {
                return $table;
            }
        }

        throw new Exception("Table $tableName not found in schema $schemaName");
    }

    public function describeTableColumns($schemaName, $tableName)
    {
        $this->fetchAll(sprintf('SHOW COLUMNS IN %s.%s', $this->quoteIdentifier($schemaName), $this->quoteIdentifier($tableName)));
    }

    public function getTableColumns($schemaName, $tableName)
    {
        return array_map(function ($column) {
            return $column['column_name'];
        }, $this->describeTableColumns($schemaName, $tableName));
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

    public function fetch($sql, $bind, callable $callback)
    {
        $stmt = odbc_prepare($this->connection, $sql);
        odbc_execute($stmt, $bind);
        while ($row = odbc_fetch_array($stmt)) {
            $callback($row);
        }
        odbc_free_result($stmt);
    }

}