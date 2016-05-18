<?php

namespace Keboola\DbImportTest;

use Keboola\Csv\CsvFile;
use Keboola\Db\Import\Exception;
use Keboola\Db\Import\Snowflake\Connection;

class SnowflakeTest extends \PHPUnit_Framework_TestCase
{
    /**
     * @var Connection
     */
    private $connection;

    private $destSchemaName = 'in.c-tests';

    private $sourceSchemaName = 'some.tests';

    const AWS_S3_BUCKET_ENV = 'AWS_S3_BUCKET';

    public function setUp()
    {
        $this->connection = new Connection([
            'host' => getenv('SNOWFLAKE_HOST'),
            'port' => getenv('SNOWFLAKE_PORT'),
            'database' => getenv('SNOWFLAKE_DATABASE'),
            'warehouse' => getenv('SNOWFLAKE_WAREHOUSE'),
            'user' => getenv('SNOWFLAKE_WAREHOUSE'),
            'password' => getenv('SNOWFLAKE_PASSWORD'),
        ]);
        $this->initData();
    }

    public function testConnectionWithoutDbAndWarehouse()
    {
        $connection = new Connection([
            'host' => getenv('SNOWFLAKE_HOST'),
            'port' => getenv('SNOWFLAKE_PORT'),
            'user' => getenv('SNOWFLAKE_WAREHOUSE'),
            'password' => getenv('SNOWFLAKE_PASSWORD'),
        ]);

        $databases = $connection->fetchAll('SHOW DATABASES');
        $this->assertNotEmpty($databases);
    }

    public function testGetPrimaryKey()
    {
        $pk = $this->connection->getTablePrimaryKey($this->destSchemaName, 'accounts-3');
        $this->assertEquals(['id'], $pk);
    }

    /**
     * @param $sourceData
     * @param $columns
     * @param $expected
     * @param $tableName
     * @param string $type
     * @dataProvider  fullImportData
     */
    public function testFullImport($sourceData, $columns, $expected, $tableName, $type = 'csv')
    {
        $import = $this->getImport($type);
        $import->setIgnoreLines(1);
        $import->import($tableName, $columns, $sourceData);


        $tableColumns = $this->connection->getTableColumns($this->destSchemaName, $tableName);
        if (!in_array('_timestamp', $columns)) {
            $tableColumns = array_filter($tableColumns, function($column) {
                return $column !== '_timestamp';
            });
        }

        $importedData = $this->fetchAll($this->destSchemaName, $tableName, $tableColumns);

        $this->assertArrayEqualsSorted($expected, $importedData, 0);
    }

    /**
     * @dataProvider incrementalImportData
     * @param \Keboola\Csv\CsvFile $initialImportFile
     * @param \Keboola\Csv\CsvFile $incrementFile
     * @param $columns
     * @param $expected
     * @param $tableName
     */
    public function testIncrementalImport(\Keboola\Csv\CsvFile $initialImportFile, \Keboola\Csv\CsvFile $incrementFile, $columns, $expected, $tableName, $rowsShouldBeUpdated)
    {
        // initial import
        $import = $this->getImport();
        $import
            ->setIgnoreLines(1)
            ->setIncremental(false)
            ->import($tableName, $columns, [$initialImportFile]);

        $timestampsByIdsAfterFullLoad = [];
        foreach ($this->fetchAll($this->destSchemaName, $tableName, ['id', '_timestamp']) as $row) {
            $timestampsByIdsAfterFullLoad[$row[0]] = $row[1];
        }


        sleep(2);
        $import
            ->setIncremental(true)
            ->import($tableName, $columns, [$incrementFile]);

        $tableColumns = $this->connection->getTableColumns($this->destSchemaName, $tableName);
        $tableColumns = array_filter($tableColumns, function($column) {
            return $column !== '_timestamp';
        });
        
        $timestampsByIdsAfterIncrement = [];
        foreach ($this->fetchAll($this->destSchemaName, $tableName, ['id', '_timestamp']) as $row) {
            $timestampsByIdsAfterIncrement[$row[0]] = $row[1];
        }

        $changedTimestamps = array_diff($timestampsByIdsAfterIncrement, $timestampsByIdsAfterFullLoad);
        $updatedRows = array_keys($changedTimestamps);
        sort($updatedRows);
        sort($rowsShouldBeUpdated);
        $this->assertEquals($rowsShouldBeUpdated, $updatedRows);

        $importedData = $this->fetchAll($this->destSchemaName, $tableName, $tableColumns);
        $this->assertArrayEqualsSorted($expected, $importedData, 0);
    }

    public function incrementalImportData()
    {
        $s3bucket = getenv(self::AWS_S3_BUCKET_ENV);
        $initialFile = new CsvFile("s3://{$s3bucket}/tw_accounts.csv");
        $incrementFile = new CsvFile("s3://{$s3bucket}/tw_accounts.increment.csv");

        $expectationFile = new CsvFile(__DIR__ . '/_data/csv-import/expectation.tw_accounts.increment.csv');
        $expectedRows = [];
        foreach ($expectationFile as $row) {
            $expectedRows[] = $row;
        }
        $columns = array_shift($expectedRows);
        $expectedRows = array_values($expectedRows);

        return [
            [$initialFile, $incrementFile, $columns, $expectedRows, 'accounts-3', [15, 24]],
        ];
    }

    public function fullImportData()
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

        $file = new \Keboola\Csv\CsvFile(__DIR__ . '/_data/csv-import/lemma.csv');
        $expectedLemma = [];
        foreach ($file as $row) {
            $expectedLemma[] = $row;
        }
        $lemmaHeader = array_shift($expectedLemma);
        $expectedLemma = array_values($expectedLemma);

        $s3bucket = getenv(self::AWS_S3_BUCKET_ENV);

        return [
            // full imports
            [[new CsvFile("s3://{$s3bucket}/lemma.csv")], $lemmaHeader, $expectedLemma, 'out.lemma'],
            [[new CsvFile("s3://{$s3bucket}/standard-with-enclosures.csv")], $escapingHeader, $expectedEscaping, 'out.csv_2Cols'],
            [[new CsvFile("s3://{$s3bucket}/gzipped-standard-with-enclosures.csv.gz")], $escapingHeader, $expectedEscaping, 'out.csv_2Cols'],
            [[new CsvFile("s3://{$s3bucket}/standard-with-enclosures.tabs.csv", "\t")], $escapingHeader, $expectedEscaping, 'out.csv_2Cols'],
            [[new CsvFile("s3://{$s3bucket}/raw.rs.csv", "\t", '', '\\')], $escapingHeader, $expectedEscaping, 'out.csv_2Cols'],
            [[new CsvFile("s3://{$s3bucket}/tw_accounts.changedColumnsOrder.csv")], $accountChangedColumnsOrderHeader, $expectedAccounts, 'accounts-3'],
            [[new CsvFile("s3://{$s3bucket}/tw_accounts.csv")], $accountsHeader, $expectedAccounts, 'accounts-3'],

            // manifests
            [[new CsvFile("s3://{$s3bucket}/01_tw_accounts.csv.manifest")], $accountsHeader, $expectedAccounts, 'accounts-3', 'manifest'],
            [[new CsvFile("s3://{$s3bucket}/03_tw_accounts.csv.gzip.manifest")], $accountsHeader, $expectedAccounts, 'accounts-3', 'manifest'],

            // copy from table
            [
                ['schemaName' => $this->sourceSchemaName, 'tableName' => 'out.csv_2Cols'],
                $escapingHeader,
                [['a', 'b'], ['c', 'd']],
                'out.csv_2Cols',
                'copy'
            ],

            [
                ['schemaName' => $this->sourceSchemaName, 'tableName' => 'types'],
                ['charCol', 'numCol', 'floatCol', 'boolCol'],
                [['a', '10.5', '0.3', 'true']],
                'types',
                'copy'
            ],

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

    public function testInvalidCsvImport()
    {
        $s3bucket = getenv(self::AWS_S3_BUCKET_ENV);
        $importFile = new \Keboola\Csv\CsvFile("s3://{$s3bucket}/tw_accounts.csv");

        $import = $this->getImport();
        $import->setIgnoreLines(1);
        try {
            $import->import('out.csv_2Cols', ['col1', 'col2'], [$importFile]);
            $this->fail('File should not be imported');
        } catch (Exception $e) {
            $this->assertEquals(Exception::INVALID_SOURCE_DATA, $e->getCode());
        }
    }

    public function testInvalidManifestImport()
    {
        $s3bucket = getenv(self::AWS_S3_BUCKET_ENV);
        $initialFile = new \Keboola\Csv\CsvFile(__DIR__ . "/_data/csv-import/tw_accounts.csv");
        $importFile = new \Keboola\Csv\CsvFile("s3://{$s3bucket}/02_tw_accounts.csv.invalid.manifest");

        $import = $this->getImport('manifest');
        $import->setIgnoreLines(1);

        try {
            $import->import('accounts-3', $initialFile->getHeader(), [$importFile]);
            $this->fail('Manifest should not be uploaded');
        } catch (\Keboola\Db\Import\Exception $e) {
            $this->assertEquals(\Keboola\Db\Import\Exception::MANDATORY_FILE_NOT_FOUND, $e->getCode());
        }
    }

    public function testCopyInvalidParamsShouldThrowException()
    {
        $import = $this->getImport('copy');

        try {
            $import->import('out.csv_2Cols', ['col1', 'col2'], []);
            $this->fail('exception should be thrown');
        } catch (\Keboola\Db\Import\Exception $e) {
            $this->assertEquals(\Keboola\Db\Import\Exception::INVALID_SOURCE_DATA, $e->getCode());
        }
    }

    public function testCopyInvalidSourceDataShouldThrowException()
    {
        $import = $this->getImport('copy');

        try {
            $import->import('out.csv_2Cols', ['c1', 'c2'], [
                    'schemaName' => $this->sourceSchemaName,
                    'tableName' => 'names']
            );
            $this->fail('exception should be thrown');
        } catch (\Keboola\Db\Import\Exception $e) {
            $this->assertEquals(\Keboola\Db\Import\Exception::COLUMNS_COUNT_NOT_MATCH, $e->getCode());
        }
    }


    private function initData()
    {
        $currentDate = new \DateTime('now', new \DateTimeZone('UTC'));
        $now = $currentDate->format('Y-m-d H:i:s');

        foreach ([$this->sourceSchemaName, $this->destSchemaName] as $schema) {
            $this->connection->query(sprintf('DROP SCHEMA IF EXISTS "%s"', $schema));
            $this->connection->query(sprintf('CREATE SCHEMA "%s"', $schema));
        }

        $this->connection->query(sprintf('CREATE TABLE "%s"."out.lemma" (
          "ts" VARCHAR,
          "lemma" VARCHAR,
          "lemmaIndex" VARCHAR,
          "_timestamp" TIMESTAMP_NTZ
        );', $this->destSchemaName));

        $this->connection->query(sprintf('CREATE TABLE "%s"."out.csv_2Cols" (
          "col1" VARCHAR,
          "col2" VARCHAR,
          "_timestamp" TIMESTAMP_NTZ
        );', $this->destSchemaName));

        $this->connection->query(sprintf('INSERT INTO "%s"."out.csv_2Cols" VALUES
                  (\'x\', \'y\', \'%s\');'
        , $this->destSchemaName, $now));

        $this->connection->query(sprintf('CREATE TABLE "%s"."out.csv_2Cols" (
          "col1" VARCHAR,
          "col2" VARCHAR
        );', $this->sourceSchemaName));


        $this->connection->query(sprintf('INSERT INTO "%s"."out.csv_2Cols" VALUES
                (\'a\', \'b\'), (\'c\', \'d\');
        ', $this->sourceSchemaName));

        $this->connection->query(sprintf(
           'CREATE TABLE "%s"."accounts-3" (
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

        $this->connection->query(sprintf(
           'CREATE TABLE "%s"."table" (
              "column"  varchar(65535),
              "table" varchar(65535),
              "_timestamp" TIMESTAMP_NTZ
            );'
        , $this->destSchemaName));

        $this->connection->query(sprintf(
            'CREATE TABLE "%s"."types" (
              "charCol"  varchar NOT NULL,
              "numCol" varchar NOT NULL,
              "floatCol" varchar NOT NULL,
              "boolCol" varchar NOT NULL,
              "_timestamp" TIMESTAMP_NTZ
            );'
        , $this->destSchemaName));

        $this->connection->query(sprintf(
            'CREATE TABLE "%s"."types" (
              "charCol"  varchar(65535) NOT NULL,
              "numCol" number(10,1) NOT NULL,
              "floatCol" float NOT NULL,
              "boolCol" boolean NOT NULL
            );'
        , $this->sourceSchemaName));

        $this->connection->query(sprintf(
            'INSERT INTO "%s"."types" VALUES 
              (\'a\', \'10.5\', \'0.3\', true)
           ;'
        , $this->sourceSchemaName));
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
            case 'copy':
                return new \Keboola\Db\Import\Snowflake\CopyImport(
                    $this->connection,
                    $this->destSchemaName
                );
            default:
                throw new \Exception("Import type $type not found");

        }
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

        return array_map(function($row) {
            return array_map(function($column) {
                return base64_decode($column);
            }, array_values($row));
        }, $this->connection->fetchAll($sql));
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
