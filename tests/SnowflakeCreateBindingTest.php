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

        $stmt = odbc_prepare($connection, "DROP TABLE IF EXISTS original;");
        odbc_execute($stmt);
        odbc_free_result($stmt);

        $stmt = odbc_prepare($connection, "CREATE TABLE original (x VARCHAR, y VARCHAR)");
        odbc_execute($stmt);
        odbc_free_result($stmt);

        $stmt = odbc_prepare($connection, "INSERT INTO original VALUES ('aaa', 'bbb'), ('ccc','ddd')");
        odbc_execute($stmt);
        odbc_free_result($stmt);


        // this is ok
        $stmt = odbc_prepare($connection, "SELECT * FROM original WHERE x = ?");
        odbc_execute($stmt, array('aaa'));
        $result = odbc_fetch_array($stmt);
        $this->assertNotEmpty($result);
        odbc_free_result($stmt);

        // this is also ok
        $stmt = odbc_prepare($connection, "SELECT * FROM original WHERE x = ?");
        odbc_execute($stmt, array('ddd'));
        $result = odbc_fetch_array($stmt);
        $this->assertEmpty($result);
        odbc_free_result($stmt);

        // this is also ok
        $stmt = odbc_prepare($connection, "SELECT * FROM original WHERE x = ?");
        odbc_execute($stmt, array("'ddd"));
        $result = odbc_fetch_array($stmt);
        $this->assertEmpty($result);
        odbc_free_result($stmt);

        // this is not ok
        // it throws - odbc_execute(): Can't open file ddd
        try {
            $stmt = odbc_prepare($connection, "SELECT * FROM original WHERE x = ?");
            odbc_execute($stmt, array("'ddd'"));
            $result = odbc_fetch_array($stmt);
            $this->assertEmpty($result);
            odbc_free_result($stmt);
        } catch (\Exception $e) {
            var_dump($e->getMessage());

        }
    }

}