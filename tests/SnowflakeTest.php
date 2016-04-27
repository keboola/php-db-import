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

        $importedData = $this->fetchAll($this->destSchemaName, $tableName, $tableColumns);

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

        $expectedAccounts = [];
        $file = new \Keboola\Csv\CsvFile(__DIR__ . '/_data/csv-import/tw_accounts.csv');
        foreach ($file as $row) {
            $expectedAccounts[] = $row;
        }
        $accountsHeader = array_shift($expectedAccounts); // remove header
        $expectedAccounts = array_values($expectedAccounts);

        $file = new \Keboola\Csv\CsvFile(__DIR__ . '/_data/csv-import/tw_accounts.changedColumnsOrder.csv');
        $accountChangedColumnsOrderHeader = $file->getHeader();


        $s3bucket = getenv(self::AWS_S3_BUCKET_ENV);

        return [
            // full imports
            [[new CsvFile("s3://{$s3bucket}/standard-with-enclosures.csv")], $escapingHeader, $expectedEscaping, 'out.csv_2Cols'],
            [[new CsvFile("s3://{$s3bucket}/gzipped-standard-with-enclosures.csv.gz")], $escapingHeader, $expectedEscaping, 'out.csv_2Cols'],
            [[new CsvFile("s3://{$s3bucket}/standard-with-enclosures.tabs.csv", "\t")], $escapingHeader, $expectedEscaping, 'out.csv_2Cols'],
            [[new CsvFile("s3://{$s3bucket}/raw.rs.csv", "\t", '', '\\')], $escapingHeader, $expectedEscaping, 'out.csv_2Cols'],
            [[new CsvFile("s3://{$s3bucket}/tw_accounts.changedColumnsOrder.csv")], $accountChangedColumnsOrderHeader, $expectedAccounts, 'accounts'],
            [[new CsvFile("s3://{$s3bucket}/tw_accounts.csv")], $accountsHeader, $expectedAccounts, 'accounts'],

            // manifests
            [[new CsvFile("s3://{$s3bucket}/01_tw_accounts.csv.manifest")], $accountsHeader, $expectedAccounts, 'accounts', 'manifest'],
            [[new CsvFile("s3://{$s3bucket}/03_tw_accounts.csv.gzip.manifest")], $accountsHeader, $expectedAccounts, 'accounts', 'manifest'],

            // reserved words
            [[new CsvFile("s3://{$s3bucket}/reserved-words.csv")], ['column', 'table'], [['table', 'column']], 'table', 'csv'],


            // import table with _timestamp columns - used by snapshots
            [
                [new CsvFile("s3://{$s3bucket}/with-ts.csv")],
                ['col1', 'col2', '_timestamp'],
                [
                    ['a', 'b', 'Mon, 10 Nov 2014 13:12:06 Z'],
                    ['c', 'd', 'Mon, 10 Nov 2014 14:12:06 Z'],
                ],
                'out.csv_2Cols'
            ],

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

        $this->query(sprintf(
           'CREATE TABLE "%s"."accounts" (
                "id" varchar(65535) NOT NULL,
                "idTwitter" varchar(65535) NOT NULL,
                "name" varchar(65535) NOT NULL,
                "import" varchar(65535) NOT NULL,
                "isImported" varchar(65535) NOT NULL,
                "apiLimitExceededDatetime" varchar(65535) NOT NULL,
                "analyzeSentiment" varchar(65535) NOT NULL,
                "importKloutScore" varchar(65535) NOT NULL,
                "timestamp" varchar(65535) NOT NULL,
                "oauthToken" varchar(65535) NOT NULL,
                "oauthSecret" varchar(65535) NOT NULL,
                "idApp" varchar(65535) NOT NULL,
                "_timestamp" TIMESTAMP_NTZ,
                PRIMARY KEY("id")
        )', $this->destSchemaName));

        $this->query(sprintf(
           'CREATE TABLE "%s"."table" (
              "column"  varchar(65535),
              "table" varchar(65535),
              "_timestamp" TIMESTAMP_NTZ
            );'
        , $this->destSchemaName));
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
            case 'manifest':
                return new \Keboola\Db\Import\Snowflake\CsvManifestImport(
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
        return odbc_exec($this->connection, $sql);
    }

    private function fetchAll($schemaName, $tableName, $columns)
    {
        // temporary fix of client charset handling
        $columnsSql = array_map(function($column) {
            return sprintf('BASE64_ENCODE("%s") AS "%s"', $column, $column);
        }, $columns);

        $sql = sprintf("SELECT %s FROM \"%s\".\"%s\"",
            implode(', ', $columnsSql),
            $schemaName,
            $tableName
        );

        $stmt = odbc_prepare($this->connection, $sql);
        odbc_execute($stmt);
        $rows = [];
        while ($row = odbc_fetch_array($stmt)) {
            $rows[] = array_map(function($column) {
                return base64_decode($column);
            }, array_values($row));
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
