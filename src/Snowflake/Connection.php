<?php

declare(strict_types=1);

namespace Keboola\Db\Import\Snowflake;

use Keboola\Db\Import\Exception;
use Throwable;

class Connection implements ConnectionInterface
{
    use ConnectionTrait;

    private const SNOWFLAKE_APPLICATION = 'Keboola_Connection';

    /**
     * @var resource odbc handle
     */
    private $connection;

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
        $maxBackoffAttempts = isset($options['maxBackoffAttempts']) ? (int) $options['maxBackoffAttempts'] : 5;

        $dsn = 'Driver=SnowflakeDSIIDriver;Server=' . $options['host'];
        $dsn .= ';Port=' . $port;
        $dsn .= ';Tracing=' . $tracing;

        if (isset($options['loginTimeout'])) {
            $dsn .= ';Login_timeout=' . (int) $options['loginTimeout'];
        }

        if (isset($options['networkTimeout'])) {
            $dsn .= ';Network_timeout=' . (int) $options['networkTimeout'];
        }

        if (isset($options['queryTimeout'])) {
            $dsn .= ';Query_timeout=' . (int) $options['queryTimeout'];
        }

        if (isset($options['database'])) {
            $dsn .= ';Database=' . $this->quoteIdentifier($options['database']);
        }

        if (isset($options['warehouse'])) {
            $dsn .= ';Warehouse=' . $this->quoteIdentifier($options['warehouse']);
        }

        $dsn .= ';application=' . $this->quoteIdentifier(self::SNOWFLAKE_APPLICATION);

        $dsn .= ';CLIENT_SESSION_KEEP_ALIVE=TRUE';

        $attemptNumber = 0;
        do {
            if ($attemptNumber > 0) {
                sleep(pow(2, $attemptNumber));
            }
            try {
                $this->connection = odbc_connect($dsn, $options['user'], $options['password']);

                if (isset($options['runId'])) {
                    $queryTag = [
                        'runId' => $options['runId'],
                    ];
                    $this->query("ALTER SESSION SET QUERY_TAG='" . json_encode($queryTag) . "';");
                }
            } catch (Throwable $e) {
                // try again if it is a failed rest request
                if (stristr($e->getMessage(), 'S1000') !== false) {
                    $attemptNumber++;
                    if ($attemptNumber > $maxBackoffAttempts) {
                        throw new Exception('Initializing Snowflake connection failed: ' . $e->getMessage(), 0, $e);
                    }
                } else {
                    throw new Exception('Initializing Snowflake connection failed: ' . $e->getMessage(), 0, $e);
                }
            }
        } while ($this->connection === null);
    }

    public function __destruct()
    {
        $this->disconnect();
    }

    public function query(string $sql, array $bind = []): void
    {
        try {
            $stmt = odbc_prepare($this->connection, $sql);
            odbc_execute($stmt, $this->repairBinding($bind));
            odbc_free_result($stmt);
        } catch (Throwable $e) {
            throw (new ExceptionHandler())->createException($e);
        }
    }

    public function fetchAll(string $sql, array $bind = []): array
    {
        try {
            $stmt = odbc_prepare($this->connection, $sql);
            odbc_execute($stmt, $this->repairBinding($bind));
            $rows = [];
            while ($row = odbc_fetch_array($stmt)) {
                $rows[] = $row;
            }
            odbc_free_result($stmt);
        } catch (Throwable $e) {
            throw (new ExceptionHandler())->createException($e);
        }
        return $rows;
    }

    public function fetch(string $sql, array $bind, callable $callback): void
    {
        try {
            $stmt = odbc_prepare($this->connection, $sql);
            odbc_execute($stmt, $this->repairBinding($bind));
            while ($row = odbc_fetch_array($stmt)) {
                $callback($row);
            }
            odbc_free_result($stmt);
        } catch (Throwable $e) {
            throw (new ExceptionHandler())->createException($e);
        }
    }

    /**
     * Avoid odbc file open http://php.net/manual/en/function.odbc-execute.php
     * @param array $bind
     * @return array
     */
    private function repairBinding(array $bind): array
    {
        return array_map(function ($value) {
            if (preg_match("/^'.*'$/", $value)) {
                return " {$value} ";
            } else {
                return $value;
            }
        }, $bind);
    }

    public function disconnect(): void
    {
        if (is_resource($this->connection)) {
            odbc_close($this->connection);
        }
    }
}
