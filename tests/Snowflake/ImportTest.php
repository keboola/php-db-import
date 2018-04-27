<?php

declare(strict_types=1);

namespace Keboola\DbImportTest\Snowflake;

use Keboola\Csv\CsvFile;
use Keboola\Db\Import\Exception;
use Keboola\Db\Import\Snowflake\Connection;

class ImportTest extends \PHPUnit_Framework_TestCase
{
    /**
     * @var Connection
     */
    private $connection;

    /** @var string  */
    private $destSchemaName = 'in.c-tests';

    /** @var string  */
    private $sourceSchemaName = 'some.tests';

    private const AWS_S3_BUCKET_ENV = 'AWS_S3_BUCKET';

    public function setUp(): void
    {
        $this->connection = new Connection([
            'host' => getenv('SNOWFLAKE_HOST'),
            'port' => getenv('SNOWFLAKE_PORT'),
            'database' => getenv('SNOWFLAKE_DATABASE'),
            'warehouse' => getenv('SNOWFLAKE_WAREHOUSE'),
            'user' => getenv('SNOWFLAKE_USER'),
            'password' => getenv('SNOWFLAKE_PASSWORD'),
        ]);
        $this->initData();
    }

    protected function tearDown(): void
    {
        $this->connection = null;
    }

    /**
     * This should not exhaust memory
     */
    public function testLargeTableIterate(): void
    {
        $generateRowsCount = 1000000;
        $this->connection->query(sprintf("
          CREATE TABLE \"bigData\" AS
            SELECT 
              uniform(1, 10, random()) as \"col1\",
              uniform(1, 10, random()) as \"col2\",
              uniform(1, 10, random()) as \"col3\"
            FROM TABLE(GENERATOR(rowCount => $generateRowsCount)) v ORDER BY 1;
        ", $this->destSchemaName));

        $results = [
            'count' => 0,
        ];
        $callback = function ($row) use (&$results): void {
            $results['count'] = $results['count'] + 1;
        };

        $this->connection->fetch(sprintf(
            "SELECT * FROM %s.%s",
            $this->connection->quoteIdentifier($this->destSchemaName),
            $this->connection->quoteIdentifier("bigData")
        ), [], $callback);

        $this->assertEquals($generateRowsCount, $results['count']);
    }

    public function testTableInfo(): void
    {
        $this->connection->query('CREATE TABLE "' . $this->destSchemaName . '"."Test" (col1 varchar, col2 varchar)');

        $table = $this->connection->describeTable($this->destSchemaName, "Test");
        $this->assertEquals("Test", $table['name']);
        $this->assertArrayHasKey('rows', $table);
        $this->assertArrayHasKey('bytes', $table);
    }


    public function testGetPrimaryKey(): void
    {
        $pk = $this->connection->getTablePrimaryKey($this->destSchemaName, 'accounts-3');
        $this->assertEquals(['id'], $pk);
    }

    /**
     * @dataProvider  fullImportData
     * @param array $sourceData
     * @param array $columns
     * @param array $expected
     * @param string $tableName
     * @param string $type
     * @param array $importOptions
     * @throws Exception
     */
    public function testFullImport(
        array $sourceData,
        array $columns,
        array $expected,
        string $tableName,
        string $type = 'csv',
        array $importOptions = ['useTimestamp' => true]
    ): void {
        $import = $this->getImport($type);
        if ($type !== 'manifest') {
            $import->setIgnoreLines(1);
        }
        $import->import($tableName, $columns, $sourceData, $importOptions);


        $tableColumns = $this->connection->getTableColumns($this->destSchemaName, $tableName);

        if ($importOptions['useTimestamp']) {
            $this->assertContains('_timestamp', $tableColumns);
        } else {
            $this->assertNotContains('_timestamp', $tableColumns);
        }

        if (!in_array('_timestamp', $columns)) {
            $tableColumns = array_filter($tableColumns, function ($column) {
                return $column !== '_timestamp';
            });
        }

        $importedData = $this->fetchAll($this->destSchemaName, $tableName, $tableColumns);

        $this->assertArrayEqualsSorted($expected, $importedData, '0');
    }

    /**
     * @dataProvider incrementalImportData
     * @param CsvFile $initialImportFile
     * @param CsvFile $incrementFile
     * @param array $columns
     * @param array $expected
     * @param string $tableName
     * @param array $importOptions
     * @throws Exception
     */
    public function testIncrementalImport(
        CsvFile $initialImportFile,
        CsvFile $incrementFile,
        array $columns,
        array $expected,
        string $tableName,
        array $importOptions = ['useTimestamp' => true]
    ): void {

        // initial import
        $import = $this->getImport();
        $import
            ->setIgnoreLines(1)
            ->setIncremental(false)
            ->import($tableName, $columns, [$initialImportFile], $importOptions);

        $import
            ->setIncremental(true)
            ->import($tableName, $columns, [$incrementFile], $importOptions);

        $tableColumns = $this->connection->getTableColumns($this->destSchemaName, $tableName);

        if ($importOptions['useTimestamp']) {
            $this->assertContains('_timestamp', $tableColumns);
        } else {
            $this->assertNotContains('_timestamp', $tableColumns);
        }

        $tableColumns = array_filter($tableColumns, function ($column) {
            return $column !== '_timestamp';
        });
        
        $importedData = $this->fetchAll($this->destSchemaName, $tableName, $tableColumns);

        $this->assertArrayEqualsSorted($expected, $importedData, '0');
    }

    public function incrementalImportData(): array
    {
        $s3bucket = getenv(self::AWS_S3_BUCKET_ENV);

        // accounts
        $initialAccountsFile = new CsvFile("s3://{$s3bucket}/tw_accounts.csv");
        $incrementAccountsFile = new CsvFile("s3://{$s3bucket}/tw_accounts.increment.csv");

        $expectationAccountsFile = new CsvFile(__DIR__ . '/../_data/csv-import/expectation.tw_accounts.increment.csv');
        $expectedAccountsRows = [];
        foreach ($expectationAccountsFile as $row) {
            $expectedAccountsRows[] = $row;
        }
        $accountColumns = array_shift($expectedAccountsRows);
        $expectedAccountsRows = array_values($expectedAccountsRows);

        // multi pk
        $initialMultiPkFile = new CsvFile("s3://{$s3bucket}/multi-pk.csv");
        $incrementMultiPkFile = new CsvFile("s3://{$s3bucket}/multi-pk.increment.csv");

        $expectationMultiPkFile = new CsvFile(__DIR__ . '/../_data/csv-import/expectation.multi-pk.increment.csv');
        $expectedMultiPkRows = [];
        foreach ($expectationMultiPkFile as $row) {
            $expectedMultiPkRows[] = $row;
        }
        $multiPkColumns = array_shift($expectedMultiPkRows);
        $expectedMultiPkRows = array_values($expectedMultiPkRows);

        return [
            [$initialAccountsFile, $incrementAccountsFile, $accountColumns, $expectedAccountsRows, 'accounts-3'],
            [$initialAccountsFile, $incrementAccountsFile, $accountColumns, $expectedAccountsRows, 'accounts-bez-ts', ['useTimestamp' => false]],
            [$initialMultiPkFile, $incrementMultiPkFile, $multiPkColumns, $expectedMultiPkRows, 'multi-pk'],
        ];
    }

    public function fullImportData(): array
    {
        $expectedEscaping = [];
        $file = new \Keboola\Csv\CsvFile(__DIR__ . '/../_data/csv-import/escaping/standard-with-enclosures.csv');
        foreach ($file as $row) {
            $expectedEscaping[] = $row;
        }
        $escapingHeader = array_shift($expectedEscaping); // remove header
        $expectedEscaping = array_values($expectedEscaping);

        $expectedAccounts = [];
        $file = new \Keboola\Csv\CsvFile(__DIR__ . '/../_data/csv-import/tw_accounts.csv');
        foreach ($file as $row) {
            $expectedAccounts[] = $row;
        }
        $accountsHeader = array_shift($expectedAccounts); // remove header
        $expectedAccounts = array_values($expectedAccounts);

        $file = new \Keboola\Csv\CsvFile(__DIR__ . '/../_data/csv-import/tw_accounts.changedColumnsOrder.csv');
        $accountChangedColumnsOrderHeader = $file->getHeader();

        $file = new \Keboola\Csv\CsvFile(__DIR__ . '/../_data/csv-import/lemma.csv');
        $expectedLemma = [];
        foreach ($file as $row) {
            $expectedLemma[] = $row;
        }
        $lemmaHeader = array_shift($expectedLemma);
        $expectedLemma = array_values($expectedLemma);

        // large sliced manifest
        $expectedLargeSlicedManifest = [];
        for ($i = 0; $i <= 1500; $i++) {
            $expectedLargeSlicedManifest[] = ['a', 'b'];
        }

        $s3bucket = getenv(self::AWS_S3_BUCKET_ENV);

        return [
            // full imports
            [[new CsvFile("s3://{$s3bucket}/manifests/2cols-large/sliced.csvmanifest")], $escapingHeader, $expectedLargeSlicedManifest, 'out.csv_2Cols', 'manifest' ],
            [[new CsvFile("s3://{$s3bucket}/empty.manifest")], $escapingHeader, [], 'out.csv_2Cols', 'manifest' ],
            [[new CsvFile("s3://{$s3bucket}/lemma.csv")], $lemmaHeader, $expectedLemma, 'out.lemma'],
            [[new CsvFile("s3://{$s3bucket}/standard-with-enclosures.csv")], $escapingHeader, $expectedEscaping, 'out.csv_2Cols'],
            [[new CsvFile("s3://{$s3bucket}/gzipped-standard-with-enclosures.csv.gz")], $escapingHeader, $expectedEscaping, 'out.csv_2Cols'],
            [[new CsvFile("s3://{$s3bucket}/standard-with-enclosures.tabs.csv", "\t")], $escapingHeader, $expectedEscaping, 'out.csv_2Cols'],
            [[new CsvFile("s3://{$s3bucket}/raw.rs.csv", "\t", '', '\\')], $escapingHeader, $expectedEscaping, 'out.csv_2Cols'],
            [[new CsvFile("s3://{$s3bucket}/tw_accounts.changedColumnsOrder.csv")], $accountChangedColumnsOrderHeader, $expectedAccounts, 'accounts-3'],
            [[new CsvFile("s3://{$s3bucket}/tw_accounts.csv")], $accountsHeader, $expectedAccounts, 'accounts-3'],

            // manifests
            [[new CsvFile("s3://{$s3bucket}/manifests/accounts/tw_accounts.csvmanifest")], $accountsHeader, $expectedAccounts, 'accounts-3', 'manifest'],
            [[new CsvFile("s3://{$s3bucket}/manifests/accounts-gzip/tw_accounts.csv.gzmanifest")], $accountsHeader, $expectedAccounts, 'accounts-3', 'manifest'],

            // copy from table
            [
                ['schemaName' => $this->sourceSchemaName, 'tableName' => 'out.csv_2Cols'],
                $escapingHeader,
                [['a', 'b'], ['c', 'd']],
                'out.csv_2Cols',
                'copy',
            ],

            [
                ['schemaName' => $this->sourceSchemaName, 'tableName' => 'types'],
                ['charCol', 'numCol', 'floatCol', 'boolCol'],
                [['a', '10.5', '0.3', 'true']],
                'types',
                'copy',
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
                'out.csv_2Cols',
            ],
            // test creating table without _timestamp column
            [
                [new CsvFile("s3://{$s3bucket}/standard-with-enclosures.csv")],
                $escapingHeader,
                $expectedEscaping,
                'out.no_timestamp_table',
                'csv',
                ['useTimestamp' => false],
            ],

        ];
    }

    public function testInvalidCsvImport(): void
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

    public function testImportShouldNotFailOnColumnNameRowNumber(): void
    {
        $s3bucket = getenv(self::AWS_S3_BUCKET_ENV);
        $importFile = new \Keboola\Csv\CsvFile("s3://{$s3bucket}/column-name-row-number.csv");

        $import = $this->getImport();
        $import->setIncremental(false);
        $import->import('column-name-row-number', ['id', 'row_number'], [$importFile]);
    }

    public function testInvalidManifestImport(): void
    {
        $s3bucket = getenv(self::AWS_S3_BUCKET_ENV);
        $initialFile = new \Keboola\Csv\CsvFile(__DIR__ . "/../_data/csv-import/tw_accounts.csv");
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

    public function testMoreColumnsShouldThrowException(): void
    {
        $s3bucket = getenv(self::AWS_S3_BUCKET_ENV);
        $importFile = new \Keboola\Csv\CsvFile("s3://{$s3bucket}/tw_accounts.csv");

        $import = $this->getImport();
        $import->setIgnoreLines(1);
        try {
            $import->import('out.csv_2Cols', ['first', 'second'], [$importFile]);
            $this->fail('File should not be imported');
        } catch (Exception $e) {
            $this->assertEquals(Exception::COLUMNS_COUNT_NOT_MATCH, $e->getCode());
            $this->assertContains('first', $e->getMessage());
            $this->assertContains('second', $e->getMessage());
        }
    }

    public function testCopyInvalidParamsShouldThrowException(): void
    {
        $import = $this->getImport('copy');

        try {
            $import->import('out.csv_2Cols', ['col1', 'col2'], []);
            $this->fail('exception should be thrown');
        } catch (\Keboola\Db\Import\Exception $e) {
            $this->assertEquals(\Keboola\Db\Import\Exception::INVALID_SOURCE_DATA, $e->getCode());
        }
    }

    public function testCopyInvalidSourceDataShouldThrowException(): void
    {
        $import = $this->getImport('copy');

        try {
            $import->import(
                'out.csv_2Cols',
                ['c1', 'c2'],
                [
                    'schemaName' => $this->sourceSchemaName,
                    'tableName' => 'names',
                ]
            );
            $this->fail('exception should be thrown');
        } catch (\Keboola\Db\Import\Exception $e) {
            $this->assertEquals(\Keboola\Db\Import\Exception::COLUMNS_COUNT_NOT_MATCH, $e->getCode());
        }
    }


    private function initData(): void
    {
        $currentDate = new \DateTime('now', new \DateTimeZone('UTC'));
        $now = $currentDate->format('Y-m-d H:i:s');

        foreach ([$this->sourceSchemaName, $this->destSchemaName] as $schema) {
            $this->connection->query(sprintf('DROP SCHEMA IF EXISTS "%s"', $schema));
            $this->connection->query(sprintf('CREATE SCHEMA "%s"', $schema));
        }

        $this->connection->query(sprintf('CREATE TABLE "%s"."out.lemma" (
          "ts" VARCHAR NOT NULL DEFAULT \'\',
          "lemma" VARCHAR NOT NULL DEFAULT \'\',
          "lemmaIndex" VARCHAR NOT NULL DEFAULT \'\',
          "_timestamp" TIMESTAMP_NTZ
        );', $this->destSchemaName));

        $this->connection->query(sprintf('CREATE TABLE "%s"."out.csv_2Cols" (
          "col1" VARCHAR NOT NULL DEFAULT \'\',
          "col2" VARCHAR NOT NULL DEFAULT \'\',
          "_timestamp" TIMESTAMP_NTZ
        );', $this->destSchemaName));

        $this->connection->query(sprintf(
            'INSERT INTO "%s"."out.csv_2Cols" VALUES
                  (\'x\', \'y\', \'%s\');',
            $this->destSchemaName,
            $now
        ));

        $this->connection->query(sprintf('CREATE TABLE "%s"."out.csv_2Cols" (
          "col1" VARCHAR NOT NULL DEFAULT \'\',
          "col2" VARCHAR NOT NULL DEFAULT \'\'
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
            )',
            $this->destSchemaName
        ));

        $this->connection->query(sprintf(
            'CREATE TABLE "%s"."accounts-bez-ts" (
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
                PRIMARY KEY("id")
            )',
            $this->destSchemaName
        ));

        $this->connection->query(sprintf(
            'CREATE TABLE "%s"."table" (
              "column"  varchar(65535) NOT NULL DEFAULT \'\',
              "table" varchar(65535) NOT NULL DEFAULT \'\',
              "_timestamp" TIMESTAMP_NTZ
            );',
            $this->destSchemaName
        ));

        $this->connection->query(sprintf(
            'CREATE TABLE "%s"."types" (
              "charCol"  varchar NOT NULL,
              "numCol" varchar NOT NULL,
              "floatCol" varchar NOT NULL,
              "boolCol" varchar NOT NULL,
              "_timestamp" TIMESTAMP_NTZ
            );',
            $this->destSchemaName
        ));

        $this->connection->query(sprintf(
            'CREATE TABLE "%s"."types" (
              "charCol"  varchar(65535) NOT NULL,
              "numCol" number(10,1) NOT NULL,
              "floatCol" float NOT NULL,
              "boolCol" boolean NOT NULL
            );',
            $this->sourceSchemaName
        ));

        $this->connection->query(sprintf(
            'INSERT INTO "%s"."types" VALUES 
              (\'a\', \'10.5\', \'0.3\', true)
           ;',
            $this->sourceSchemaName
        ));

        $this->connection->query(sprintf(
            'CREATE TABLE "%s"."out.no_timestamp_table" (
              "col1" VARCHAR NOT NULL DEFAULT \'\',
              "col2" VARCHAR NOT NULL DEFAULT \'\'
            );',
            $this->destSchemaName
        ));

        $this->connection->query(sprintf(
            'CREATE TABLE "%s"."column-name-row-number" (
              "id" varchar(65535) NOT NULL,
              "row_number" varchar(65535) NOT NULL,
              "_timestamp" TIMESTAMP_NTZ,
              PRIMARY KEY("id")
            );',
            $this->destSchemaName
        ));

        $this->connection->query(sprintf(
            'CREATE TABLE "%s"."multi-pk" (
            "VisitID" VARCHAR NOT NULL DEFAULT \'\',
            "Value" VARCHAR NOT NULL DEFAULT \'\',
            "MenuItem" VARCHAR NOT NULL DEFAULT \'\',
            "Something" VARCHAR NOT NULL DEFAULT \'\',
            "Other" VARCHAR NOT NULL DEFAULT \'\',
            "_timestamp" TIMESTAMP_NTZ,
            PRIMARY KEY("VisitID","Value","MenuItem")
            );',
            $this->destSchemaName
        ));
    }


    private function getImport(string $type = 'csv'): \Keboola\Db\Import\ImportInterface
    {
        switch ($type) {
            case 'csv':
                return new \Keboola\Db\Import\Snowflake\CsvImport(
                    $this->connection,
                    getenv('AWS_ACCESS_KEY_ID'),
                    getenv('AWS_SECRET_ACCESS_KEY'),
                    getenv('AWS_REGION'),
                    $this->destSchemaName
                );
            case 'manifest':
                return new \Keboola\Db\Import\Snowflake\CsvManifestImport(
                    $this->connection,
                    getenv('AWS_ACCESS_KEY_ID'),
                    getenv('AWS_SECRET_ACCESS_KEY'),
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

    public function testNullifyCsv(): void
    {
        $s3bucket = getenv(self::AWS_S3_BUCKET_ENV);
        $this->connection->query("DROP TABLE IF EXISTS \"$this->destSchemaName\".\"nullify\" ");
        $this->connection->query("CREATE TABLE \"$this->destSchemaName\".\"nullify\" (\"id\" VARCHAR, \"name\" VARCHAR, \"price\" NUMERIC)");

        $import = $this->getImport('csv');
        $import->setIgnoreLines(1);
        $import->import(
            'nullify',
            ['id', 'name', 'price'],
            [
                new CsvFile("s3://{$s3bucket}/nullify.csv"),
            ],
            [
                "useTimestamp" => false,
                "convertEmptyValuesToNull" => ["name", "price"],
            ]
        );

        $importedData = $this->connection->fetchAll("SELECT \"id\", \"name\", \"price\" FROM \"nullify\" ORDER BY \"id\" ASC");
        $this->assertCount(3, $importedData);
        $this->assertTrue(null === $importedData[1]["name"]);
        $this->assertTrue(null === $importedData[2]["price"]);
    }


    public function testNullifyCsvIncremental(): void
    {
        $s3bucket = getenv(self::AWS_S3_BUCKET_ENV);
        $this->connection->query("DROP TABLE IF EXISTS \"$this->destSchemaName\".\"nullify\" ");
        $this->connection->query("CREATE TABLE \"$this->destSchemaName\".\"nullify\" (\"id\" VARCHAR, \"name\" VARCHAR, \"price\" NUMERIC)");
        $this->connection->query("INSERT INTO \"$this->destSchemaName\".\"nullify\" VALUES('4', NULL, 50)");

        $import = $this->getImport('csv');
        $import->setIgnoreLines(1);
        $import->setIncremental(true);
        $import->import(
            'nullify',
            ['id', 'name', 'price'],
            [
                new CsvFile("s3://{$s3bucket}/nullify.csv"),
            ],
            [
                "useTimestamp" => false,
                "convertEmptyValuesToNull" => ["name", "price"],
            ]
        );

        $importedData = $this->connection->fetchAll("SELECT \"id\", \"name\", \"price\" FROM \"nullify\" ORDER BY \"id\" ASC");
        $this->assertCount(4, $importedData);
        $this->assertTrue(null === $importedData[1]["name"]);
        $this->assertTrue(null === $importedData[2]["price"]);
        $this->assertTrue(null === $importedData[3]["name"]);
    }

    public function testNullifyCopy(): void
    {
        $this->connection->query("DROP TABLE IF EXISTS \"$this->destSchemaName\".\"nullify\" ");
        $this->connection->query("CREATE TABLE \"$this->destSchemaName\".\"nullify\" (\"id\" VARCHAR, \"name\" VARCHAR, \"price\" NUMERIC)");
        $this->connection->query("DROP TABLE IF EXISTS \"$this->destSchemaName\".\"nullify_src\" ");
        $this->connection->query("CREATE TABLE \"$this->destSchemaName\".\"nullify_src\" (\"id\" VARCHAR, \"name\" VARCHAR, \"price\" NUMERIC)");
        $this->connection->query("INSERT INTO \"$this->destSchemaName\".\"nullify_src\" VALUES('1', '', NULL), ('2', NULL, 500)");

        $import = $this->getImport('copy');
        $import->setIgnoreLines(1);
        $import->import(
            'nullify',
            ['id', 'name', 'price'],
            [
                "tableName" => "nullify_src",
                "schemaName" => $this->destSchemaName,
            ],
            [
                "useTimestamp" => false,
                "convertEmptyValuesToNull" => ["name", "price"],
            ]
        );

        $importedData = $this->connection->fetchAll("SELECT \"id\", \"name\", \"price\" FROM \"nullify\" ORDER BY \"id\" ASC");
        $this->assertCount(2, $importedData);
        $this->assertTrue(null === $importedData[0]["name"]);
        $this->assertTrue(null === $importedData[0]["price"]);
        $this->assertTrue(null === $importedData[1]["name"]);
    }

    public function testNullifyCopyIncremental(): void
    {
        $this->connection->query("DROP TABLE IF EXISTS \"$this->destSchemaName\".\"nullify\" ");
        $this->connection->query("CREATE TABLE \"$this->destSchemaName\".\"nullify\" (\"id\" VARCHAR, \"name\" VARCHAR, \"price\" NUMERIC)");
        $this->connection->query("INSERT INTO \"$this->destSchemaName\".\"nullify\" VALUES('4', NULL, 50)");
        $this->connection->query("DROP TABLE IF EXISTS \"$this->destSchemaName\".\"nullify_src\" ");
        $this->connection->query("CREATE TABLE \"$this->destSchemaName\".\"nullify_src\" (\"id\" VARCHAR, \"name\" VARCHAR, \"price\" NUMERIC)");
        $this->connection->query("INSERT INTO \"$this->destSchemaName\".\"nullify_src\" VALUES('1', '', NULL), ('2', NULL, 500)");

        $import = $this->getImport('copy');
        $import->setIgnoreLines(1);
        $import->setIncremental(true);
        $import->import(
            'nullify',
            ['id', 'name', 'price'],
            [
                "tableName" => "nullify_src",
                "schemaName" => $this->destSchemaName,
            ],
            [
                "useTimestamp" => false,
                "convertEmptyValuesToNull" => ["name", "price"],
            ]
        );

        $importedData = $this->connection->fetchAll("SELECT \"id\", \"name\", \"price\" FROM \"nullify\" ORDER BY \"id\" ASC");
        $this->assertCount(3, $importedData);
        $this->assertTrue(null === $importedData[0]["name"]);
        $this->assertTrue(null === $importedData[0]["price"]);
        $this->assertTrue(null === $importedData[1]["name"]);
        $this->assertTrue(null === $importedData[2]["name"]);
    }

    public function testNullifyCopyIncrementalWithPk(): void
    {
        $this->connection->query("DROP TABLE IF EXISTS \"$this->destSchemaName\".\"nullify\" ");
        $this->connection->query("CREATE TABLE \"$this->destSchemaName\".\"nullify\" (\"id\" VARCHAR, \"name\" VARCHAR, \"price\" NUMERIC, PRIMARY KEY(\"id\"))");
        $this->connection->query("INSERT INTO \"$this->destSchemaName\".\"nullify\" VALUES('4', '3', 2)");
        $this->connection->query("DROP TABLE IF EXISTS \"$this->destSchemaName\".\"nullify_src\" ");
        $this->connection->query("CREATE TABLE \"$this->destSchemaName\".\"nullify_src\" (\"id\" VARCHAR NOT NULL, \"name\" VARCHAR NOT NULL, \"price\" VARCHAR NOT NULL, PRIMARY KEY(\"id\"))");
        $this->connection->query("INSERT INTO \"$this->destSchemaName\".\"nullify_src\" VALUES('1', '', ''), ('2', '', '500'), ('4', '', '')");

        $import = $this->getImport('copy');
        $import->setIgnoreLines(1);
        $import->setIncremental(true);
        $import->import(
            'nullify',
            ['id', 'name', 'price'],
            [
                "tableName" => "nullify_src",
                "schemaName" => $this->destSchemaName,
            ],
            [
                "useTimestamp" => false,
                "convertEmptyValuesToNull" => ["name", "price"],
            ]
        );

        $importedData = $this->connection->fetchAll("SELECT \"id\", \"name\", \"price\" FROM \"nullify\"");

        $expectedData = [
            [
                'id' => '1',
                'name' => null,
                'price' => null,
            ],
            [
                'id' => '2',
                'name' => null,
                'price' => '500',
            ],
            [
                'id' => '4',
                'name' => null,
                'price' => null,
            ],

        ];

        $this->assertArrayEqualsSorted($expectedData, $importedData, 'id');
    }

    public function testNullifyCopyIncrementalWithPkDestinationWithNull(): void
    {
        $this->connection->query("DROP TABLE IF EXISTS \"$this->destSchemaName\".\"nullify\" ");
        $this->connection->query("CREATE TABLE \"$this->destSchemaName\".\"nullify\" (\"id\" VARCHAR, \"name\" VARCHAR, \"price\" NUMERIC, PRIMARY KEY(\"id\"))");
        $this->connection->query("INSERT INTO \"$this->destSchemaName\".\"nullify\" VALUES('4', NULL, NULL)");
        $this->connection->query("DROP TABLE IF EXISTS \"$this->destSchemaName\".\"nullify_src\" ");
        $this->connection->query("CREATE TABLE \"$this->destSchemaName\".\"nullify_src\" (\"id\" VARCHAR NOT NULL, \"name\" VARCHAR NOT NULL, \"price\" VARCHAR NOT NULL, PRIMARY KEY(\"id\"))");
        $this->connection->query("INSERT INTO \"$this->destSchemaName\".\"nullify_src\" VALUES('1', '', ''), ('2', '', '500'), ('4', '', '500')");

        $import = $this->getImport('copy');
        $import->setIgnoreLines(1);
        $import->setIncremental(true);
        $import->import(
            'nullify',
            ['id', 'name', 'price'],
            [
                "tableName" => "nullify_src",
                "schemaName" => $this->destSchemaName,
            ],
            [
                "useTimestamp" => false,
                "convertEmptyValuesToNull" => ["name", "price"],
            ]
        );

        $importedData = $this->connection->fetchAll("SELECT \"id\", \"name\", \"price\" FROM \"nullify\"");

        $expectedData = [
            [
                'id' => '1',
                'name' => null,
                'price' => null,
            ],
            [
                'id' => '2',
                'name' => null,
                'price' => '500',
            ],
            [
                'id' => '4',
                'name' => null,
                'price' => '500',
            ],

        ];

        $this->assertArrayEqualsSorted($expectedData, $importedData, 'id');
    }

    private function fetchAll(string $schemaName, string $tableName, array $columns): array
    {
        // temporary fix of client charset handling
        $columnsSql = array_map(function ($column) {
            return sprintf('BASE64_ENCODE("%s") AS "%s"', $column, $column);
        }, $columns);

        $sql = sprintf(
            "SELECT %s FROM \"%s\".\"%s\"",
            implode(', ', $columnsSql),
            $schemaName,
            $tableName
        );

        return array_map(function ($row) {
            return array_map(function ($column) {
                return base64_decode($column);
            }, array_values($row));
        }, $this->connection->fetchAll($sql));
    }

    public function assertArrayEqualsSorted(array $expected, array $actual, string $sortKey, string $message = ""): void
    {
        $comparsion = function ($attrLeft, $attrRight) use ($sortKey) {
            if ($attrLeft[$sortKey] == $attrRight[$sortKey]) {
                return 0;
            }
            return $attrLeft[$sortKey] < $attrRight[$sortKey] ? -1 : 1;
        };
        usort($expected, $comparsion);
        usort($actual, $comparsion);
        $this->assertEquals($expected, $actual, $message);
    }
}
