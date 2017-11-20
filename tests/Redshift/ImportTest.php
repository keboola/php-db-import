<?php

namespace Keboola\DbImportTest\Redshift;

use Keboola\Csv\CsvFile;
use Keboola\Db\Import\Exception;

class ImportTest extends \PHPUnit_Framework_TestCase
{
    /**
     * @var \PDO
     */
    protected $connection;

    private $destSchemaName = 'in.c-tests';

    private $sourceSchemaName = 'some.tests';

    const AWS_S3_BUCKET_ENV = 'AWS_S3_BUCKET';

    public static function setUpBeforeClass()
    {
        echo sprintf("\nLegacy mode: %s\n", (bool)  getenv('REDSHIFT_LEGACY_IMPORT') ? 'ON' : 'OFF');
    }

    public function setUp()
    {
        $this->connection = $pdo = new \PDO(
            sprintf('pgsql:host=%s;dbname=%s;port=%s', getenv('REDSHIFT_HOST'), getenv('REDSHIFT_DATABASE'), getenv('REDSHIFT_PORT')),
            getenv('REDSHIFT_USER'),
            getenv('REDSHIFT_PASSWORD'),
            [
                \PDO::MYSQL_ATTR_LOCAL_INFILE => true,
                \PDO::ATTR_ERRMODE => \PDO::ERRMODE_EXCEPTION,
            ]
        );
        $this->initData();
    }

    private function initData()
    {
        $commands = [];

        $schemas = [$this->sourceSchemaName, $this->destSchemaName];

        $currentDate = new \DateTime('now', new \DateTimeZone('UTC'));
        $now = $currentDate->format('Ymd H:i:s');

        foreach ($schemas as $schema) {
            $tablesToDelete = [
                'out.csv_2Cols',
                'accounts',
                'types',
                'names',
                'with_ts',
                'table',
                'random',
                'out.no_timestamp_table',
                'accounts_bez_ts',
                'column_name_row_number',
                'multi-pk',
            ];
            foreach ($tablesToDelete as $tableToDelete) {
                $stmt = $this->connection
                    ->prepare("SELECT table_name FROM information_schema.tables WHERE table_name = ? AND table_schema = ?");

                if ($stmt->execute([strtolower($tableToDelete), strtolower($schema)]) && $stmt->fetch()) {
                    $commands[] = "DROP TABLE \"$schema\".\"$tableToDelete\"";
                }
            }

            $stmt = $this->connection->prepare("SELECT * FROM pg_catalog.pg_namespace where nspname = ?");
            if ($stmt->execute([$schema]) && !$stmt->fetch()) {
                $commands[] = "
                    CREATE SCHEMA \"$schema\";
                ";
            }

            $commands[] = "
                CREATE TABLE \"$schema\".\"out.csv_2Cols\" (
                  col1  varchar(65535),
                  col2 varchar(65535),
                  _timestamp TIMESTAMP
                );
            ";

            $commands[] = "
                INSERT INTO \"$schema\".\"out.csv_2Cols\" VALUES
                    ('a', 'b', '{$now}')
                ;
            ";

            $commands[] =  "
                CREATE TABLE \"$schema\".\"table\" (
                  \"column\"  varchar(65535),
                  \"table\" varchar(65535),
                  _timestamp TIMESTAMP
                );
            ";

            $commands[] = "
                CREATE TABLE \"$schema\".accounts (
                    id varchar(65535) NOT NULL,
                    idTwitter varchar(65535) NOT NULL,
                    name varchar(65535) NOT NULL,
                    import varchar(65535) NOT NULL,
                    isImported varchar(65535) NOT NULL,
                    apiLimitExceededDatetime varchar(65535) NOT NULL,
                    analyzeSentiment varchar(65535) NOT NULL,
                    importKloutScore varchar(65535) NOT NULL,
                    timestamp varchar(65535) NOT NULL,
                    oauthToken varchar(65535) NOT NULL,
                    oauthSecret varchar(65535) NOT NULL,
                    idApp varchar(65535) NOT NULL,
                    _timestamp TIMESTAMP,
                    PRIMARY KEY(id)
                );
            ";

            $commands[] = "
                CREATE TABLE \"$schema\".accounts_bez_ts (
                    id varchar(65535) NOT NULL,
                    idTwitter varchar(65535) NOT NULL,
                    name varchar(65535) NOT NULL,
                    import varchar(65535) NOT NULL,
                    isImported varchar(65535) NOT NULL,
                    apiLimitExceededDatetime varchar(65535) NOT NULL,
                    analyzeSentiment varchar(65535) NOT NULL,
                    importKloutScore varchar(65535) NOT NULL,
                    timestamp varchar(65535) NOT NULL,
                    oauthToken varchar(65535) NOT NULL,
                    oauthSecret varchar(65535) NOT NULL,
                    idApp varchar(65535) NOT NULL,
                    PRIMARY KEY(id)
                );
            ";

            $commands[] =  "
                CREATE TABLE \"$schema\".\"out.no_timestamp_table\" (
                  col1  varchar(65535),
                  col2 varchar(65535)
                );
            ";
        }

        $commands[] = "
            CREATE TABLE \"{$this->sourceSchemaName}\".types (
                  col1  varchar(65535) NOT NULL,
                  col2 boolean NOT NULL
                );
        ";

        $commands[] = "
            CREATE TABLE \"{$this->destSchemaName}\".types (
                  col1  varchar(65535) NOT NULL,
                  col2 varchar(65535) NOT NULL,
                  _timestamp TIMESTAMP
                );
        ";

        $commands[] = "
            INSERT INTO \"{$this->sourceSchemaName}\".\"out.csv_2Cols\" VALUES
                ('c', 'd')
            ;
        ";

        $commands[] = "
            INSERT INTO \"{$this->sourceSchemaName}\".types VALUES
                ('c', 'true'),
                ('d', 'false')
            ;
        ";

        $commands[] = "
            CREATE TABLE \"{$this->sourceSchemaName}\".names (
                  col1  varchar(65535) NOT NULL,
                  col2  varchar(65535)
                );
        ";

        $commands[] = "
            INSERT INTO \"{$this->sourceSchemaName}\".names VALUES
                ('c', 'true'),
                ('d', NULL)
            ;
        ";

        $commands[] = "
            CREATE TABLE \"$schema\".column_name_row_number (
                  id  varchar(65535) NOT NULL,
                  row_number  varchar(65535) NOT NULL,
                  _timestamp TIMESTAMP,
                  PRIMARY KEY(id)
                );
        ";

        $commands[] = "
            CREATE TABLE  \"{$this->destSchemaName}\".\"multi-pk\" (
            \"VisitID\" VARCHAR NOT NULL,
            \"Value\" VARCHAR NOT NULL,
            \"MenuItem\" VARCHAR NOT NULL,
            \"Something\" VARCHAR NOT NULL,
            \"Other\" VARCHAR NOT NULL,
              _timestamp TIMESTAMP,
            PRIMARY KEY(\"VisitID\",\"Value\",\"MenuItem\")
           );";

        foreach ($commands as $command) {
            $this->connection->query($command);
        }
    }


    /**
     * @dataProvider tables
     */
    public function testFullImport($sourceData, $columns, $expected, $tableName, $type = 'csv', $importOptions = ['useTimestamp' => true])
    {

        $import = $this->getImport($type);
        if ($type !== 'manifest') {
            $import->setIgnoreLines(1);
        }

        $result = $import->import($tableName, $columns, $sourceData, $importOptions);
        $this->assertEquals((bool) getenv('REDSHIFT_LEGACY_IMPORT'), $result->getKeyValue('legacyFullImport'));

        $tableColumns = $this->describeTable(strtolower($tableName), strtolower($this->destSchemaName));

        if ($importOptions['useTimestamp']) {
            $this->assertArrayHasKey('_timestamp', $tableColumns);
        } else {
            $this->assertArrayNotHasKey('_timestamp', $tableColumns);
        }

        if (!in_array('_timestamp', $columns)) {
            unset($tableColumns['_timestamp']);
        }

        $columnsSql = implode(", ", array_map(function ($column) {
            return '"' . $column . '"';
        }, array_keys($tableColumns)));

        $importedData = $this->connection->query("SELECT $columnsSql FROM \"{$this->destSchemaName}\".\"$tableName\"")->fetchAll(\PDO::FETCH_NUM);

        $this->assertArrayEqualsSorted($expected, $importedData, 0);
    }

    public function testImportShouldNotFailOnColumnNameRowNumber()
    {
        $s3bucket = getenv(self::AWS_S3_BUCKET_ENV);

        $import = $this->getImport('csv');
        $import->setIncremental(false);

        $import->import(
            'column_name_row_number',
            [
                'id',
                'row_number',
            ],
            [new CsvFile("s3://{$s3bucket}/column-name-row-number.csv")],
            ['useTimestamp' => false]
        );
    }

    /**
     * @dataProvider tablesIncremental
     * @param \Keboola\Csv\CsvFile $initialImportFile
     * @param \Keboola\Csv\CsvFile $incrementFile
     * @param $columns
     * @param $expected
     * @param $tableName
     */
    public function testIncrementalImport(
        \Keboola\Csv\CsvFile $initialImportFile,
        \Keboola\Csv\CsvFile $incrementFile,
        $columns,
        $expected,
        $tableName,
        $importOptions = ['useTimestamp' => true]
    ) {
        // initial import
        $import = $this->getImport();
        $import
            ->setIgnoreLines(1)
            ->setIncremental(false)
            ->import($tableName, $columns, [$initialImportFile], $importOptions);

        $import
            ->setIncremental(true)
            ->import($tableName, $columns, [$incrementFile], $importOptions);

        $tableColumns = $this->describeTable($tableName, strtolower($this->destSchemaName));
        if ($importOptions['useTimestamp']) {
            $this->assertArrayHasKey('_timestamp', $tableColumns);
            unset($tableColumns['_timestamp']);
        } else {
            $this->assertArrayNotHasKey('_timestamp', $tableColumns);
        }

        $columnsSql = implode(", ", array_map(function ($column) {
            return '"' . $column . '"';
        }, array_keys($tableColumns)));

        $importedData = $this->connection->query("SELECT $columnsSql FROM \"{$this->destSchemaName}\".\"$tableName\"")->fetchAll(\PDO::FETCH_NUM);
        $this->assertArrayEqualsSorted($expected, $importedData, 0);
    }


    public function testCopyOptions()
    {
        $s3bucket = getenv(self::AWS_S3_BUCKET_ENV);
        $this->connection->query("DROP TABLE IF EXISTS \"$this->destSchemaName\".\"dates\" ");
        $this->connection->query("CREATE TABLE \"$this->destSchemaName\".\"dates\" (valid_from DATETIME,  _timestamp TIMESTAMP)");

        $import = $this->getImport('csv');

        $import->import(
            'dates',
            ['valid_from'],
            [
                new CsvFile("s3://{$s3bucket}/dates.csv"),
            ],
            [
                'useTimestamp' => true,
                'copyOptions' => [
                    'NULL AS \'NULL\'',
                    'ACCEPTANYDATE',
                    'TRUNCATECOLUMNS',
                ]
            ]
        );

        $importedData = $this->connection->query("SELECT valid_from FROM \"{$this->destSchemaName}\".\"dates\"")->fetchAll();
        $this->assertCount(4, $importedData);
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

    public function testQueryTimeoutError()
    {
        $import = $this->getImport('copy');

        $this->connection->query("CREATE TABLE \"{$this->sourceSchemaName}\".\"random\" (
          \"col1\"  varchar(65535) NOT NULL,
          \"col2\"  varchar(65535) NOT NULL
        );");

        $this->connection->query(sprintf(
            "INSERT INTO \"{$this->sourceSchemaName}\".\"random\" 
              VALUES %s
            ",
            implode(',', array_map(function ($number) {
                return "($number, $number)";
            }, range(0, 10000)))
        ));

        $this->connection->query('set statement_timeout to 1;');

        try {
            $import->import('out.csv_2Cols', ['col1', 'col2'], [
                'schemaName' => $this->sourceSchemaName,
                'tableName' => 'random'
            ]);
            $this->fail();
        } catch (\Keboola\Db\Import\Exception $e) {
            $this->assertEquals(\Keboola\Db\Import\Exception::QUERY_TIMEOUT, $e->getCode());
        }
    }


    public function testCopyInvalidSourceDataShouldThrowException()
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

    public function testInvalidManifestImport()
    {
        $s3bucket = getenv(self::AWS_S3_BUCKET_ENV);
        $initialFile = new \Keboola\Csv\CsvFile(__DIR__ . "/../_data/csv-import/tw_accounts.csv");
        $importFile = new \Keboola\Csv\CsvFile("s3://{$s3bucket}/02_tw_accounts.csv.invalid.manifest");

        $import = $this->getImport('manifest');
        $import->setIgnoreLines(1);

        try {
            $import->import('accounts', $initialFile->getHeader(), [$importFile]);
        } catch (\Keboola\Db\Import\Exception $e) {
            $this->assertEquals(\Keboola\Db\Import\Exception::MANDATORY_FILE_NOT_FOUND, $e->getCode());
        }
    }

    public function testNullifyCsv()
    {
        $s3bucket = getenv(self::AWS_S3_BUCKET_ENV);
        $this->connection->query("DROP TABLE IF EXISTS \"$this->destSchemaName\".\"nullify\" ");
        $this->connection->query("CREATE TABLE \"$this->destSchemaName\".\"nullify\" (id VARCHAR, name VARCHAR, price INTEGER)");

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
                "convertEmptyValuesToNull" => ["name", "price"]
            ]
        );

        $importedData = $this->connection->query("SELECT id, name, price FROM \"{$this->destSchemaName}\".\"nullify\" ORDER BY id ASC")->fetchAll();
        $this->assertCount(3, $importedData);
        $this->assertTrue(null === $importedData[1]["name"]);
        $this->assertTrue(null === $importedData[2]["price"]);
    }
    
    public function testNullifyCsvIncremental()
    {
        $s3bucket = getenv(self::AWS_S3_BUCKET_ENV);
        $this->connection->query("DROP TABLE IF EXISTS \"$this->destSchemaName\".\"nullify\" ");
        $this->connection->query("CREATE TABLE \"$this->destSchemaName\".\"nullify\" (id VARCHAR, name VARCHAR, price INTEGER)");
        $this->connection->query("INSERT INTO \"$this->destSchemaName\".\"nullify\" VALUES('4', NULL, NULL)");

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
                "convertEmptyValuesToNull" => ["name", "price"]
            ]
        );

        $importedData = $this->connection->query("SELECT id, name, price FROM \"{$this->destSchemaName}\".\"nullify\" ORDER BY id ASC")->fetchAll();
        $this->assertCount(4, $importedData);
        $this->assertTrue(null === $importedData[1]["name"]);
        $this->assertTrue(null === $importedData[2]["price"]);
        $this->assertTrue(null === $importedData[3]["name"]);
        $this->assertTrue(null === $importedData[3]["price"]);
    }

    public function testNullifyCopy()
    {
        $this->connection->query("DROP TABLE IF EXISTS \"$this->destSchemaName\".\"nullify\" ");
        $this->connection->query("CREATE TABLE \"$this->destSchemaName\".\"nullify\" (id VARCHAR, name VARCHAR, price VARCHAR)");
        $this->connection->query("DROP TABLE IF EXISTS \"$this->destSchemaName\".\"nullify_src\" ");
        $this->connection->query("CREATE TABLE \"$this->destSchemaName\".\"nullify_src\" (id VARCHAR, name VARCHAR, price VARCHAR)");
        $this->connection->query("INSERT INTO \"$this->destSchemaName\".\"nullify_src\" VALUES('1', '', NULL), ('2', NULL, 500)");

        $import = $this->getImport('copy');
        $import->setIgnoreLines(1);
        $import->import(
            'nullify',
            ['id', 'name', 'price'],
            [
                "tableName" => "nullify_src",
                "schemaName" => $this->destSchemaName
            ],
            [
                "useTimestamp" => false,
                "convertEmptyValuesToNull" => ["name", "price"]
            ]
        );

        $importedData = $this->connection->query("SELECT id, name, price FROM \"{$this->destSchemaName}\".\"nullify\" ORDER BY id ASC")->fetchAll();
        $this->assertCount(2, $importedData);
        $this->assertTrue(null === $importedData[0]["name"]);
        $this->assertTrue(null === $importedData[0]["price"]);
        $this->assertTrue(null === $importedData[1]["name"]);
    }

    public function testNullifyCopyIncremental()
    {
        $this->connection->query("DROP TABLE IF EXISTS \"$this->destSchemaName\".\"nullify\" ");
        $this->connection->query("CREATE TABLE \"$this->destSchemaName\".\"nullify\" (id VARCHAR, name VARCHAR, price VARCHAR)");
        $this->connection->query("INSERT INTO \"$this->destSchemaName\".\"nullify\" VALUES('3', NULL, 5)");
        $this->connection->query("DROP TABLE IF EXISTS \"$this->destSchemaName\".\"nullify_src\" ");
        $this->connection->query("CREATE TABLE \"$this->destSchemaName\".\"nullify_src\" (id VARCHAR, name VARCHAR, price VARCHAR)");
        $this->connection->query("INSERT INTO \"$this->destSchemaName\".\"nullify_src\" VALUES('1', '', NULL), ('2', NULL, 500)");

        $import = $this->getImport('copy');
        $import->setIgnoreLines(1);
        $import->setIncremental(true);
        $import->import(
            'nullify',
            ['id', 'name', 'price'],
            [
                "tableName" => "nullify_src",
                "schemaName" => $this->destSchemaName
            ],
            [
                "useTimestamp" => false,
                "convertEmptyValuesToNull" => ["name", "price"]
            ]
        );

        $importedData = $this->connection->query("SELECT id, name, price FROM \"{$this->destSchemaName}\".\"nullify\" ORDER BY id ASC")->fetchAll();
        $this->assertCount(3, $importedData);
        $this->assertTrue(null === $importedData[0]["name"]);
        $this->assertTrue(null === $importedData[1]["name"]);
        $this->assertTrue(null === $importedData[2]["name"]);
        $this->assertTrue(null === $importedData[0]["price"]);
    }


    public function testNullifyCopyIncrementalWithPk()
    {
        // test convertEmptyValuesToNull
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
                "schemaName" => $this->destSchemaName
            ],
            [
                "useTimestamp" => false,
                "convertEmptyValuesToNull" => ["name", "price"]
            ]
        );

        $importedData = $this->connection->query("SELECT \"id\", \"name\", \"price\" FROM \"{$this->destSchemaName}\".\"nullify\" ORDER BY \"id\" ASC")->fetchAll(\PDO::FETCH_ASSOC);
        $this->assertCount(3, $importedData);
        $this->assertTrue(null === $importedData[0]["name"]);
        $this->assertTrue(null === $importedData[0]["price"]);
        $this->assertTrue(null === $importedData[1]["name"]);
        $this->assertTrue(null === $importedData[2]["name"]);
        $this->assertTrue(null === $importedData[2]["price"]);

        // test apply change if destination contains null
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
                "schemaName" => $this->destSchemaName
            ],
            [
                "useTimestamp" => false,
                "convertEmptyValuesToNull" => ["name", "price"]
            ]
        );

        $importedData = $this->connection->query("SELECT \"id\", \"name\", \"price\" FROM \"{$this->destSchemaName}\".\"nullify\" ORDER BY \"id\" ASC")->fetchAll(\PDO::FETCH_ASSOC);
        $this->assertCount(3, $importedData);
        $this->assertTrue(null === $importedData[0]["name"]);
        $this->assertTrue(null === $importedData[0]["price"]);
        $this->assertTrue(null === $importedData[1]["name"]);
        $this->assertTrue(null === $importedData[2]["name"]);
        $this->assertTrue(null !== $importedData[2]["price"]);
    }

    public function tables()
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

        $s3bucket = getenv(self::AWS_S3_BUCKET_ENV);

        return [

            // full imports
            [[new CsvFile("s3://{$s3bucket}/empty.manifest")], $escapingHeader, [], 'out.csv_2Cols', 'manifest' ],
            [[new CsvFile("s3://{$s3bucket}/standard-with-enclosures.csv")], $escapingHeader, $expectedEscaping, 'out.csv_2Cols'],
            [[new CsvFile("s3://{$s3bucket}/gzipped-standard-with-enclosures.csv.gz")], $escapingHeader, $expectedEscaping, 'out.csv_2Cols'],
            [[new CsvFile("s3://{$s3bucket}/standard-with-enclosures.tabs.csv", "\t")], $escapingHeader, $expectedEscaping, 'out.csv_2Cols'],
            [[new CsvFile("s3://{$s3bucket}/raw.rs.csv", "\t", '', '\\')], $escapingHeader, $expectedEscaping, 'out.csv_2Cols'],
            [[new CsvFile("s3://{$s3bucket}/tw_accounts.changedColumnsOrder.csv")], $accountChangedColumnsOrderHeader, $expectedAccounts, 'accounts'],
            [[new CsvFile("s3://{$s3bucket}/tw_accounts.csv")], $accountsHeader, $expectedAccounts, 'accounts'],
            [[new CsvFile("s3://{$s3bucket}/manifests/accounts/tw_accounts.csvmanifest")], $accountsHeader, $expectedAccounts, 'accounts', 'manifest'],
            [[new CsvFile("s3://{$s3bucket}/manifests/accounts-gzip/tw_accounts.csv.gzmanifest")], $accountsHeader, $expectedAccounts, 'accounts', 'manifest'],

            [['schemaName' => $this->sourceSchemaName, 'tableName' => 'out.csv_2Cols'], $escapingHeader, [['a', 'b'], ['c', 'd']], 'out.csv_2Cols', 'copy'],
            [['schemaName' => $this->sourceSchemaName, 'tableName' => 'types'], $escapingHeader, [['c', '1'], ['d', '0']], 'types', 'copy'],

            // reserved words
            [[new CsvFile("s3://{$s3bucket}/reserved-words.csv")], ['column', 'table'], [['table', 'column']], 'table', 'csv'],


            // import table with _timestamp columns - used by snapshots
            [
                [new CsvFile("s3://{$s3bucket}/with-ts.csv")],
                ['col1', 'col2', '_timestamp'],
                [
                    ['a', 'b', '2014-11-10 13:12:06'],
                    ['c', 'd', '2014-11-10 14:12:06'],
                ],
                'out.csv_2Cols'
            ],
            // test creating table without _timestamp column
            [
                [new CsvFile("s3://{$s3bucket}/standard-with-enclosures.csv")],
                $escapingHeader,
                $expectedEscaping,
                'out.no_timestamp_table',
                'csv',
                ['useTimestamp' => false]
            ]
        ];
    }


    public function tablesIncremental()
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
            [$initialAccountsFile, $incrementAccountsFile, $accountColumns, $expectedAccountsRows, 'accounts'],
            [$initialAccountsFile, $incrementAccountsFile, $accountColumns, $expectedAccountsRows, 'accounts_bez_ts', ['useTimestamp' => false]],
            [$initialMultiPkFile, $incrementMultiPkFile, $multiPkColumns, $expectedMultiPkRows, 'multi-pk']
        ];
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

    /**
     * @param string $type
     * @return \Keboola\Db\Import\ImportInterface
     * @throws Exception
     */
    private function getImport($type = 'csv')
    {
        switch ($type) {
            case 'manifest':
                return new \Keboola\Db\Import\CsvManifestImportRedshift(
                    $this->connection,
                    getenv('AWS_ACCESS_KEY'),
                    getenv('AWS_SECRET_KEY'),
                    getenv('AWS_REGION'),
                    $this->destSchemaName,
                    getenv('REDSHIFT_LEGACY_IMPORT')
                );
                break;
            case 'csv':
                return new \Keboola\Db\Import\CsvImportRedshift(
                    $this->connection,
                    getenv('AWS_ACCESS_KEY'),
                    getenv('AWS_SECRET_KEY'),
                    getenv('AWS_REGION'),
                    $this->destSchemaName,
                    getenv('REDSHIFT_LEGACY_IMPORT')
                );
            case 'copy':
                return new \Keboola\Db\Import\CopyImportRedshift(
                    $this->connection,
                    $this->destSchemaName,
                    getenv('REDSHIFT_LEGACY_IMPORT')
                );
                break;
            default:
                throw new \Exception("Import type $type not found");
        }
    }

    private function describeTable($tableName, $schemaName = null)
    {
        $sql = "SELECT
                a.attnum,
                n.nspname,
                c.relname,
                a.attname AS colname,
                t.typname AS type,
                a.atttypmod,
                FORMAT_TYPE(a.atttypid, a.atttypmod) AS complete_type,
                d.adsrc AS default_value,
                a.attnotnull AS notnull,
                a.attlen AS length,
                co.contype,
                ARRAY_TO_STRING(co.conkey, ',') AS conkey
            FROM pg_attribute AS a
                JOIN pg_class AS c ON a.attrelid = c.oid
                JOIN pg_namespace AS n ON c.relnamespace = n.oid
                JOIN pg_type AS t ON a.atttypid = t.oid
                LEFT OUTER JOIN pg_constraint AS co ON (co.conrelid = c.oid
                    AND a.attnum = ANY(co.conkey) AND co.contype = 'p')
                LEFT OUTER JOIN pg_attrdef AS d ON d.adrelid = c.oid AND d.adnum = a.attnum
            WHERE a.attnum > 0 AND c.relname = " . $this->connection->quote($tableName);
        if ($schemaName) {
            $sql .= " AND n.nspname = " . $this->connection->quote($schemaName);
        }
        $sql .= ' ORDER BY a.attnum';

        $stmt = $this->connection->query($sql);

        // Use FETCH_NUM so we are not dependent on the CASE attribute of the PDO connection
        $result = $stmt->fetchAll();

        $attnum = 0;
        $nspname = 1;
        $relname = 2;
        $colname = 3;
        $type = 4;
        $atttypemod = 5;
        $complete_type = 6;
        $default_value = 7;
        $notnull = 8;
        $length = 9;
        $contype = 10;
        $conkey = 11;

        $desc = [];
        foreach ($result as $key => $row) {
            $defaultValue = $row[$default_value];
            if ($row[$type] == 'varchar' || $row[$type] == 'bpchar') {
                if (preg_match('/character(?: varying)?(?:\((\d+)\))?/', $row[$complete_type], $matches)) {
                    if (isset($matches[1])) {
                        $row[$length] = $matches[1];
                    } else {
                        $row[$length] = null; // unlimited
                    }
                }
                if (preg_match("/^'(.*?)'::(?:character varying|bpchar)$/", $defaultValue, $matches)) {
                    $defaultValue = $matches[1];
                }
            }
            list($primary, $primaryPosition, $identity) = [false, null, false];
            if ($row[$contype] == 'p') {
                $primary = true;
                $primaryPosition = array_search($row[$attnum], explode(',', $row[$conkey])) + 1;
                $identity = (bool)(preg_match('/^nextval/', $row[$default_value]));
            }
            $desc[$row[$colname]] = [
                'SCHEMA_NAME' => $row[$nspname],
                'TABLE_NAME' => $row[$relname],
                'COLUMN_NAME' => $row[$colname],
                'COLUMN_POSITION' => $row[$attnum],
                'DATA_TYPE' => $row[$type],
                'DEFAULT' => $defaultValue,
                'NULLABLE' => (bool)($row[$notnull] != 't'),
                'LENGTH' => $row[$length],
                'SCALE' => null, // @todo
                'PRECISION' => null, // @todo
                'UNSIGNED' => null, // @todo
                'PRIMARY' => $primary,
                'PRIMARY_POSITION' => $primaryPosition,
                'IDENTITY' => $identity
            ];
        }
        return $desc;
    }
}
