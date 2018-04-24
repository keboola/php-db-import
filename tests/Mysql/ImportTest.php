<?php

declare(strict_types=1);

namespace Keboola\DbImportTest\Mysql;

use Keboola\Csv\CsvFile;
use Keboola\Db\Import\CsvImportMysql;
use Keboola\DbImportTest\Helpers;

class ImportTest extends \PHPUnit_Extensions_Database_TestCase
{

    /**
     * @var \Keboola\Db\Import\CsvImportMysql
     */
    protected $import;

    public function getConnection()
    {
        $pdo = new \PDO(
            sprintf('mysql:host=%s;dbname=%s', getenv('MYSQL_HOST'), getenv('MYSQL_DATABASE')),
            'root',
            getenv('MYSQL_PASSWORD'),
            [
                \PDO::MYSQL_ATTR_LOCAL_INFILE => true,
                \PDO::ATTR_ERRMODE => \PDO::ERRMODE_EXCEPTION,
            ]
        );
        return $this->createDefaultDBConnection($pdo);
    }

    public function setUp()
    {
        // init test table - table is altered during tests - must be recreated before each test run
        Helpers::loadFromFile($this->getConnection()->getConnection(), __DIR__ . '/../_data/csv-import/init.sql');

        parent::setUp();

        $this->import = new CsvImportMysql($this->getConnection()->getConnection());
    }

    /**
     * Returns the test dataset.
     *
     * @return PHPUnit_Extensions_Database_DataSet_IDataSet
     */
    protected function getDataSet()
    {
        return $this->createMySQLXMLDataSet(__DIR__ . '/../_data/csv-import/fixtures.xml');
    }

    /**
     * @dataProvider tables
     */
    public function testImport(CsvFile $csvFile, string $expectationsFile, string $tableName, bool $incremental)
    {
        $result = $this->import
            ->setIncremental($incremental)
            ->setIgnoreLines(1)
            ->import($tableName, $csvFile->getHeader(), [$csvFile]);

        $expectedDataset = $this->createMySQLXMLDataSet(__DIR__ . "/../_data/csv-import/$expectationsFile");
        $currentDataset = $this->getConnection()->createDataSet();

        $expectedDataset = new \PHPUnit_Extensions_Database_DataSet_DataSetFilter($expectedDataset, [$tableName]);
        $expectedDataset->setExcludeColumnsForTable($tableName, ['timestamp']);

        $currentDataset = new \PHPUnit_Extensions_Database_DataSet_DataSetFilter($currentDataset, [$tableName]);
        $currentDataset->setExcludeColumnsForTable($tableName, ['timestamp']);

        $this->assertTablesEqual($expectedDataset->getTable($tableName), $currentDataset->getTable($tableName));
        $this->assertEmpty($result->getWarnings());
    }

    public function testImportWithoutHeaders()
    {
        $tableName = 'csv_2cols';
        $result = $this->import
            ->setIgnoreLines(0)
            ->import($tableName, ['col1', 'col2'], [new CsvFile(__DIR__ . '/../_data/csv-import/escaping/raw-without-headers.csv', "\t", "", "\\")]);

        $expectedDataset = $this->createMySQLXMLDataSet(__DIR__ . "/../_data/csv-import/escaping/expectation.standard.xml");
        $currentDataset = $this->getConnection()->createDataSet();

        $expectedDataset = new \PHPUnit_Extensions_Database_DataSet_DataSetFilter($expectedDataset, [$tableName]);
        $expectedDataset->setExcludeColumnsForTable($tableName, ['timestamp']);

        $currentDataset = new \PHPUnit_Extensions_Database_DataSet_DataSetFilter($currentDataset, [$tableName]);
        $currentDataset->setExcludeColumnsForTable($tableName, ['timestamp']);

        $this->assertTablesEqual($expectedDataset->getTable($tableName), $currentDataset->getTable($tableName));
        $this->assertEmpty($result->getWarnings());
    }

    public function testMultipleFilesImport()
    {
        $csvFile = new CsvFile(__DIR__ . '/../_data/csv-import/escaping/raw.csv', "\t", "", "\\");
        $importFiles = [
            $csvFile,
            $csvFile,
        ];
        $tableName = 'csv_2cols';
        $result = $this->import
            ->setIgnoreLines(1)
            ->import($tableName, ['col1', 'col2'], $importFiles);

        $this->assertEquals(7, $result->getImportedRowsCount());
    }

    public function testImportMultipleFilesPrimaryKeyDedupe()
    {
        $this->getConnection()->getConnection()->query("DROP TABLE IF EXISTS `pk_test`");
        $this->getConnection()->getConnection()->query("CREATE TABLE `pk_test` (id VARCHAR(255) NOT NULL, name VARCHAR(255) NOT NULL, PRIMARY KEY (`id`))");

        $this->import
            ->setIgnoreLines(0)
            ->import(
                'pk_test',
                ['id', 'name'],
                [
                    new CsvFile(__DIR__ . '/../_data/csv-import/pk_test.part-1.csv'),
                    new CsvFile(__DIR__ . '/../_data/csv-import/pk_test.part-2.csv'),
                ]
            );

        $importedData = $this->getConnection()->getConnection()->query("SELECT id, name FROM `pk_test` ORDER BY id ASC")->fetchAll(\PDO::FETCH_ASSOC);
        $this->assertCount(5, $importedData);

        $importedDataById = array_reduce($importedData, function ($result, $item) {
            $result[$item['id']] = $item;
            return $result;
        }, []);
        $this->assertInternalType('string', $importedDataById[4]['name']);
        $this->assertEmpty($importedDataById[4]['name']);
        $this->assertInternalType('string', $importedDataById[5]['name']);
        $this->assertEmpty($importedDataById[5]['name']);
    }

    public function testImportMultipleFilesPrimaryKeyDedupeAndNullify()
    {
        $this->getConnection()->getConnection()->query("DROP TABLE IF EXISTS `pk_test`");
        $this->getConnection()->getConnection()->query("CREATE TABLE `pk_test` (id VARCHAR(255) NOT NULL, name VARCHAR(255), PRIMARY KEY (`id`))");

        $this->import
            ->setIgnoreLines(0)
            ->import(
                'pk_test',
                ['id', 'name'],
                [
                    new CsvFile(__DIR__ . '/../_data/csv-import/pk_test.part-1.csv'),
                    new CsvFile(__DIR__ . '/../_data/csv-import/pk_test.part-2.csv'),
                ],
                [
                    "convertEmptyValuesToNull" => ["name"],
                ]
            );

        $importedData = $this->getConnection()->getConnection()->query("SELECT id, name FROM `pk_test` ORDER BY id ASC")->fetchAll(\PDO::FETCH_ASSOC);
        $this->assertCount(5, $importedData);
        $importedDataById = array_reduce($importedData, function ($result, $item) {
            $result[$item['id']] = $item;
            return $result;
        }, []);
        $this->assertNull($importedDataById[4]['name']);
        $this->assertNull($importedDataById[5]['name']);
    }

    /**
     * @TODO fix this, exception should not be thrown
     */
    public function _testImportWithWarnings()
    {
        $importFiles = [
            new CsvFile(__DIR__ . '/../_data/csv-import/escaping/raw-warnings.csv', "\t", "", "\\"),
            new CsvFile(__DIR__ . '/../_data/csv-import/escaping/standard.csv'),
        ];
        $tableName = 'csv_2cols';
        $result = $this->import
            ->setIgnoreLines(1)
            ->import($tableName, ['col1', 'col2'], $importFiles);

        $this->assertEquals(7, $result->getImportedRowsCount());
        $this->assertCount(1, $result->getWarnings());
    }


    public function duplicateColumnsData()
    {
        return [
            ['tw_accounts.duplicateColumnsAdded.csv'],
            ['tw_accounts.duplicateColumnsAdded2.csv'],
        ];
    }


    public function testInvalidTableImportShouldThrowException()
    {
        $csvFile = new CsvFile(__DIR__ . "/../_data/csv-import/tw_accounts.csv");

        $this->expectException("Keboola\\Db\\Import\\Exception");
        $this->expectExceptionCode(\Keboola\Db\Import\Exception::TABLE_NOT_EXISTS);

        $this->import
            ->setIncremental(true)
            ->setIgnoreLines(1)
            ->import('tw_something', $csvFile->getHeader(), [$csvFile]);
    }

    public function testEmptyFileShouldThrowsException()
    {
        $csvFile = new CsvFile(__DIR__ . "/../_data/csv-import/empty.csv");

        $this->expectException("Keboola\\Db\\Import\\Exception");
        $this->expectExceptionCode(\Keboola\Db\Import\Exception::NO_COLUMNS);

        $this->import
            ->setIgnoreLines(1)
            ->import('csv_accounts', $csvFile->getHeader(), [$csvFile]);
    }

    public function testEmptyFilePartialShouldThrowsException()
    {
        $csvFile = new CsvFile(__DIR__ . "/../_data/csv-import/empty.csv");

        $this->expectException("Keboola\\Db\\Import\\Exception");
        $this->expectExceptionCode(\Keboola\Db\Import\Exception::NO_COLUMNS);

        $this->import
            ->import('csv_accounts', $csvFile->getHeader(), [$csvFile]);
    }

    public function testRowTooLongShouldThrowException()
    {
        $csvFile = new CsvFile(__DIR__ . "/../_data/csv-import/very-long-row.csv");

        $this->expectException("Keboola\\Db\\Import\\Exception");
        $this->expectExceptionCode(\Keboola\Db\Import\Exception::ROW_SIZE_TOO_LARGE);

        $this->import
            ->import('very-long-row', $csvFile->getHeader(), [$csvFile]);
    }


    public function testNullifyCsv()
    {

        $this->getConnection()->getConnection()->query("DROP TABLE IF EXISTS `nullify`");
        $this->getConnection()->getConnection()->query("CREATE TABLE `nullify` (id VARCHAR(255), name VARCHAR(255), price VARCHAR(255))");

        $import = $this->import;
        $import->setIgnoreLines(1);
        $import->import(
            'nullify',
            ['id', 'name', 'price'],
            [
                new CsvFile(__DIR__ . "/../_data/csv-import/nullify.csv"),
            ],
            [
                "convertEmptyValuesToNull" => ["name", "price"],
            ]
        );

        $importedData = $this->getConnection()->getConnection()->query("SELECT id, name, price FROM `nullify` ORDER BY id ASC")->fetchAll();
        $this->assertCount(3, $importedData);
        $this->assertTrue(null === $importedData[1]["name"]);
        $this->assertTrue(null === $importedData[2]["price"]);
    }

    public function testNullifyCsvIncremental()
    {
        $this->getConnection()->getConnection()->query("DROP TABLE IF EXISTS `nullify`");
        $this->getConnection()->getConnection()->query("CREATE TABLE `nullify` (id VARCHAR(255), name VARCHAR(255), price VARCHAR(255))");
        $this->getConnection()->getConnection()->query("INSERT INTO `nullify` VALUES('4', NULL, 5)");

        $import = $this->import;
        $import->setIgnoreLines(1);
        $import->setIncremental(true);
        $import->import(
            'nullify',
            ['id', 'name', 'price'],
            [
                new CsvFile(__DIR__ . "/../_data/csv-import/nullify.csv"),
            ],
            [
                "convertEmptyValuesToNull" => ["name", "price"],
            ]
        );

        $importedData = $this->getConnection()->getConnection()->query("SELECT id, name, price FROM `nullify` ORDER BY id ASC")->fetchAll();
        $this->assertCount(4, $importedData);
        $this->assertTrue(null === $importedData[1]["name"]);
        $this->assertTrue(null === $importedData[2]["price"]);
        $this->assertTrue(null === $importedData[3]["name"]);
    }

    public function tables()
    {
        return [

            // full imports
            [new CsvFile(__DIR__ . '/../_data/csv-import/tw_accounts.csv'), 'expectation.fullImport.xml', 'csv_accounts', false, []],
            [new CsvFile(__DIR__ . '/../_data/csv-import/tw_accounts.nl-last-row.csv'), 'expectation.fullImport.xml', 'csv_accounts', false, []],
            [new CsvFile(__DIR__ . '/../_data/csv-import/tw_accounts.tabs.csv', "\t"), 'expectation.fullImport.xml', 'csv_accounts', false, []],
            [new CsvFile(__DIR__ . '/../_data/csv-import/tw_accounts.tabs.csv', "	"), 'expectation.fullImport.xml', 'csv_accounts', false, []],
            [new CsvFile(__DIR__ . '/../_data/csv-import/tw_accounts.extraColumns.csv'), 'expectation.fullImport.xml', 'csv_accounts', false, []],
            [new CsvFile(__DIR__ . '/../_data/csv-import/tw_accounts.changedColumnsOrder.csv'), 'expectation.fullImport.xml', 'csv_accounts', false, []],
            [new CsvFile(__DIR__ . '/../_data/csv-import/escaping/standard.csv'), 'escaping/expectation.standard.xml', 'csv_2cols', false, []],
            [new CsvFile(__DIR__ . '/../_data/csv-import/escaping/raw.csv', "\t", "", "\\"), 'escaping/expectation.standard.xml', 'csv_2cols', false, []],

            // line breaks
            [new CsvFile(__DIR__ . '/../_data/csv-import/csv_breaks.win.csv'), 'expectation.fullImport.xml', 'csv_breaks', false, []],
            [new CsvFile(__DIR__ . '/../_data/csv-import/csv_breaks.unix.csv'), 'expectation.fullImport.xml', 'csv_breaks', false, []],

            // reserved words
            [new CsvFile(__DIR__ . '/../_data/csv-import/reserved-words.csv'), 'expectation.reserved-words.xml', 'table-with-dash', false, []],

            // incremental imports
            [new CsvFile(__DIR__ . '/../_data/csv-import/tw_accounts.csv'), 'expectation.incrementalImport.xml', 'csv_accounts', true, []],
            [new CsvFile(__DIR__ . '/../_data/csv-import/tw_accounts.tabs.csv', "\t"), 'expectation.incrementalImport.xml', 'csv_accounts', true, []],
            [new CsvFile(__DIR__ . '/../_data/csv-import/tw_accounts.tabs.csv', "	"), 'expectation.incrementalImport.xml', 'csv_accounts', true, []],
            [new CsvFile(__DIR__ . '/../_data/csv-import/tw_accounts.extraColumns.csv'), 'expectation.incrementalImport.xml', 'csv_accounts', true, []],
            [new CsvFile(__DIR__ . '/../_data/csv-import/tw_accounts.changedColumnsOrder.csv'), 'expectation.incrementalImport.xml', 'csv_accounts', true, []],

            // specified columns import
            [new CsvFile(__DIR__ . '/../_data/csv-import/tw_accounts.columnImport.csv'), 'expectation.incrementalImportColumnsList.xml', 'csv_accounts',
                true, [], true,
            ],
            [new CsvFile(__DIR__ . '/../_data/csv-import/tw_accounts.columnImportIsImported.csv'), 'expectation.incrementalImportColumnsListIsImported.xml',
                'csv_accounts', true, [], true,
            ],
        ];
    }
}
