<?php
/**
 * Created by PhpStorm.
 * User: marc
 * Date: 02/08/2016
 * Time: 15:35
 */

namespace Keboola\DbImportTest;

class SnowflakeCreateBindingTest extends \PHPUnit_Framework_TestCase
{
    private function getConnection(array $options)
    {
        $requiredOptions = [
            'host',
            'user',
            'password',
        ];

        $missingOptions = array_diff($requiredOptions, array_keys($options));
        if (!empty($missingOptions)) {
            throw new \Exception('Missing options: ' . implode(', ', $missingOptions));
        }

        $port = isset($options['port']) ? (int) $options['port'] : 443;

        $dsn = "Driver=SnowflakeDSIIDriver;Server=" . $options['host'];
        $dsn .= ";Port=" . $port;

        if (isset($options['database'])) {
            $dsn .= ";Database=" . $options['database'];
        }

        if (isset($options['warehouse'])) {
            $dsn .= ";Warehouse=" . $options['warehouse'];
        }

        return odbc_connect($dsn, $options['user'], $options['password']);
    }

    public function testCreateBinding()
    {
        $options = array(
            "host" => getenv('SNOWFLAKE_HOST'),
            "user" => getenv('SNOWFLAKE_USER'),
            "password" => getenv('SNOWFLAKE_PASSWORD'),
            "database" => getenv('SNOWFLAKE_DATABASE'),
            "warehouse" => getenv('SNOWFLAKE_WAREHOUSE')
        );
        $connection = $this->getConnection($options);

        $stmt = odbc_prepare($connection, "USE DATABASE \"{$options['database']}\"");
        odbc_execute($stmt);
        odbc_free_result($stmt);

        $stmt = odbc_prepare($connection, "CREATE SCHEMA IF NOT EXISTS binding_test;");
        odbc_execute($stmt);
        odbc_free_result($stmt);

        $stmt = odbc_prepare($connection, "USE SCHEMA binding_test;");
        odbc_execute($stmt);
        odbc_free_result($stmt);

        $stmt = odbc_prepare($connection, "CREATE TABLE original (x NUMBER, y NUMBER)");
        odbc_execute($stmt);
        odbc_free_result($stmt);

        $stmt = odbc_prepare($connection, "INSERT INTO original VALUES (1,23), (2,46)");
        odbc_execute($stmt);
        odbc_free_result($stmt);

        // This will throw an exception
        try {
            $stmt = odbc_prepare($connection, "CREATE TABLE destination (x NUMBER, y NUMBER) AS SELECT * FROM original WHERE y = ?");
            $this->fail("This will fail, but should it?");
            // It won't get to apply this execution binding
            odbc_execute($stmt, array(23));
            odbc_free_result($stmt);

        } catch (\Exception $e) {
            // odbc_prepare(): SQL error: SQL compilation error: error line 1 at position 82
            // Bind variable ? not set., SQL state 42601 in SQLPrepare
            echo "\n\n" . $e->getMessage();
        }

        $stmt = odbc_prepare($connection, "DROP TABLE original;");
        odbc_execute($stmt);
        odbc_free_result($stmt);

        $stmt = odbc_prepare($connection, "DROP TABLE IF EXISTS destination;");
        odbc_execute($stmt);
        odbc_free_result($stmt);

        $stmt = odbc_prepare($connection, "DROP SCHEMA binding_test;");
        odbc_execute($stmt);
        odbc_free_result($stmt);
    }
}