<?php

namespace Keboola\DbImportTest;

use Keboola\Csv\CsvFile;
use Keboola\Db\Import\Exception;

class SnowflakeTest extends \PHPUnit_Framework_TestCase
{
    protected $connection;

    private $destSchemaName = 'in.c-tests';

    private $sourceSchemaName = 'some.tests';

    const AWS_S3_BUCKET_ENV = 'AWS_S3_BUCKET';

    public function setUp()
    {
        $dsn = "Driver=SnowflakeDSIIDriver;Server=" . getenv('SNOWFLAKE_HOST');
        $dsn .= ";Port=" . getenv('SNOWFLAKE_PORT');
        $dsn .= ";database=" . getenv('SNOWFLAKE_DATABASE');
        $dsn .= ";Warehouse=" . getenv('SNOWFLAKE_WAREHOUSE');
        $dsn .= ";Tracing=4";
        $dsn .= ";Query_Timeout=60";
        $connection = odbc_connect($dsn, getenv('SNOWFLAKE_USER'), getenv('SNOWFLAKE_PASSWORD'));
        try {
            odbc_exec($connection, "USE DATABASE " . getenv('SNOWFLAKE_DATABASE'));
            odbc_exec($connection, "USE WAREHOUSE " . getenv('SNOWFLAKE_WAREHOUSE'));
        } catch (\Exception $e) {
            throw new \Exception("Initializing Snowflake connection failed: " . $e->getMessage(), null, $e);
        }

        $this->connection = $connection;
        $this->initData();
    }

    /**
     * @param $sourceData
     * @param $columns
     * @param $expected
     * @param $tableName
     * @param string $type
     * @dataProvider  importData
     */
    public function testImport($sourceData, $columns, $expected, $tableName, $type = 'csv')
    {
        $import = $this->getImport($type);
        $import->setIgnoreLines(1);
        $import->import($tableName, $columns, $sourceData);


        $tableColumns = $this->tableColumns($tableName, $this->destSchemaName);
        if (!in_array('_timestamp', $columns)) {
            $tableColumns = array_filter($tableColumns, function($column) {
                return $column !== '_timestamp';
            });
        }

        $columnsSql = implode(", ", array_map(function ($column) {
            return '"' . $column . '"';
        }, $tableColumns));

        $importedData = $this->fetchAll("SELECT $columnsSql FROM \"{$this->destSchemaName}\".\"$tableName\"");

        $this->assertArrayEqualsSorted($expected, $importedData, 0);

    }

    public function importData()
    {
        $expectedEscaping = [];
        $file = new \Keboola\Csv\CsvFile(__DIR__ . '/_data/csv-import/escaping/standard-with-enclosures.csv');
        foreach ($file as $row) {
            $expectedEscaping[] = $row;
        }
        $escapingHeader = array_shift($expectedEscaping); // remove header
        $expectedEscaping = array_values($expectedEscaping);

        $s3bucket = getenv(self::AWS_S3_BUCKET_ENV);

        return [
            // full imports
            [[new CsvFile("s3://{$s3bucket}/standard-with-enclosures.csv")], $escapingHeader, $expectedEscaping, 'out.csv_2Cols'],
        ];
    }

    private function initData()
    {
        foreach ([$this->sourceSchemaName, $this->destSchemaName] as $schema) {
            $this->query(sprintf('DROP SCHEMA IF EXISTS "%s"', $schema));
            $this->query(sprintf('CREATE SCHEMA "%s"', $schema));
        }

        $this->query(sprintf('CREATE TABLE "%s"."out.csv_2Cols" (
          "col1" VARCHAR,
          "col2" VARCHAR,
          "_timestamp" TIMESTAMP_NTZ
        );', $this->destSchemaName));
    }

    private function tableColumns($tableName, $schemaName)
    {
        $res = $this->query(sprintf('SHOW COLUMNS IN "%s"."%s"', $schemaName, $tableName));
        $columns = [];
        while ($row = odbc_fetch_array($res)) {
            $columns[] = $row['column_name'];
        }
        odbc_free_result($res);
        return $columns;
    }


    /**
     * @param string $type
     * @return \Keboola\Db\Import\ImportInterface
     * @throws Exception
     */
    private function getImport($type = 'csv')
    {
        switch ($type) {
            case 'csv':
                return new \Keboola\Db\Import\Snowflake\CsvImport(
                    $this->connection,
                    getenv('AWS_ACCESS_KEY'),
                    getenv('AWS_SECRET_KEY'),
                    getenv('AWS_REGION'),
                    $this->destSchemaName
                );
            default:
                throw new \Exception("Import type $type not found");

        }
    }

    private function query($sql)
    {
        echo $sql . "\n";
        return odbc_exec($this->connection, $sql);
    }

    private function fetchAll($sql, $bind = [])
    {
        $stmt = odbc_prepare($this->connection, $sql);
        odbc_execute($stmt, $bind);
        $rows = [];
        while ($row = odbc_fetch_array($stmt)) {
            $rows[] = array_values($row);
        }
        odbc_free_result($stmt);
        return $rows;
    }

    public function assertArrayEqualsSorted($expected, $actual, $sortKey, $message = "")
    {
        $comparsion = function ($attrLeft, $attrRight) use ($sortKey) {
            if ($attrLeft[$sortKey] == $attrRight[$sortKey]) {
                return 0;
            }
            return $attrLeft[$sortKey] < $attrRight[$sortKey] ? -1 : 1;
        };
        usort($expected, $comparsion);
        usort($actual, $comparsion);
        return $this->assertEquals($expected, $actual, $message);
    }
}
