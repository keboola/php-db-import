<?php

$dsn = "Driver=SnowflakeDSIIDriver;Server=" .  getenv('SNOWFLAKE_HOST');
$dsn .= ";Port=443";
$dsn .= ";Tracing=6";
$dsn .= ";Database=\"". getenv('SNOWFLAKE_DATABASE') . "\"";
$dsn .= ";Warehouse=\"". getenv('SNOWFLAKE_WAREHOUSE') . "\"";


$connection = odbc_connect($dsn, getenv('SNOWFLAKE_USER'), getenv('SNOWFLAKE_PASSWORD'));

function fetchAll($connection, $sql, $bind = [])
{
    $stmt = odbc_prepare($connection, $sql);
    odbc_execute($stmt, $bind);
    $rows = [];
    while ($row = odbc_fetch_array($stmt)) {
        $rows[] = $row;
    }
    odbc_free_result($stmt);
    return $rows;
}

function query($connection, $sql, $bind = [])
{
    $stmt = odbc_prepare($connection, $sql);
    odbc_execute($stmt, $bind);
    odbc_free_result($stmt);
}

query($connection, "DROP SCHEMA IF EXISTS test");
query($connection, "CREATE SCHEMA test");
query($connection, "USE SCHEMA test");
query($connection, 'CREATE TABLE test (col1 varchar, col2 varchar)');
query($connection, 'INSERT INTO  test VALUES (\'šperky.cz\', \'módní doplňky.cz\')');
var_dump(fetchAll($connection, "select * from test"));
