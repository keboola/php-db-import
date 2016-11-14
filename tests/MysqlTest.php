<?php

namespace Keboola\DbImportTest;

use Keboola\Csv\CsvFile;
use Keboola\Db\Import\CsvImportMysql;

class CsvImportMysqlTest extends \PHPUnit_Extensions_Database_TestCase
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
        Helpers::loadFromFile($this->getConnection()->getConnection(), __DIR__ . '/_data/csv-import/init.sql');

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
        return $this->createMySQLXMLDataSet(__DIR__ . '/_data/csv-import/fixtures.xml');
    }

    /**
     * @dataProvider tables
     */
    public function testImport(CsvFile $csvFile, $expectationsFile, $tableName, $incremental)
    {
        $result = $this->import
            ->setIncremental($incremental)
            ->setIgnoreLines(1)
            ->import($tableName, $csvFile->getHeader(), [$csvFile]);

        $expectedDataset = $this->createMySQLXMLDataSet(__DIR__ . "/_data/csv-import/$expectationsFile");
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
            ->import($tableName, ['col1', 'col2'], [new CsvFile(__DIR__ . '/_data/csv-import/escaping/raw-without-headers.csv', "\t", "", "\\")]);

        $expectedDataset = $this->createMySQLXMLDataSet(__DIR__ . "/_data/csv-import/escaping/expectation.standard.xml");
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
        $csvFile = new CsvFile(__DIR__ . '/_data/csv-import/escaping/raw.csv', "\t", "", "\\");
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

    /**
     * @TODO fix this, exception should not be thrown
     */
    public function _testImportWithWarnings()
    {
        $importFiles = [
            new CsvFile(__DIR__ . '/_data/csv-import/escaping/raw-warnings.csv', "\t", "", "\\"),
            new CsvFile(__DIR__ . '/_data/csv-import/escaping/standard.csv'),
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
        $csvFile = new CsvFile(__DIR__ . "/_data/csv-import/tw_accounts.csv");
        $this->setExpectedException("Keboola\Db\Import\Exception", '', \Keboola\Db\Import\Exception::TABLE_NOT_EXISTS);
        $this->import
            ->setIncremental(true)
            ->setIgnoreLines(1)
            ->import('tw_something', $csvFile->getHeader(), [$csvFile]);
    }

    public function testEmptyFileShouldThrowsException()
    {
        $csvFile = new CsvFile(__DIR__ . "/_data/csv-import/empty.csv");
        $this->setExpectedException("Keboola\Db\Import\Exception", '', \Keboola\Db\Import\Exception::NO_COLUMNS);
        $this->import
            ->setIgnoreLines(1)
            ->import('csv_accounts', $csvFile->getHeader(), [$csvFile]);
    }

    public function testEmptyFilePartialShouldThrowsException()
    {
        $csvFile = new CsvFile(__DIR__ . "/_data/csv-import/empty.csv");
        $this->setExpectedException("Keboola\Db\Import\Exception", '', \Keboola\Db\Import\Exception::NO_COLUMNS);
        $this->import
            ->import('csv_accounts', $csvFile->getHeader(), [$csvFile]);
    }

    public function testRowTooLongShouldThrowException()
    {
        $csvFile = new CsvFile(__DIR__ . "/_data/csv-import/very-long-row.csv");
        $this->setExpectedException("Keboola\Db\Import\Exception", '', \Keboola\Db\Import\Exception::ROW_SIZE_TOO_LARGE);
        $this->import
            ->import('very-long-row', $csvFile->getHeader(), [$csvFile]);
    }

    public function tables()
    {
        return [

            // full imports
            [new CsvFile(__DIR__ . '/_data/csv-import/tw_accounts.csv'), 'expectation.fullImport.xml', 'csv_accounts', false, []],
            [new CsvFile(__DIR__ . '/_data/csv-import/tw_accounts.nl-last-row.csv'), 'expectation.fullImport.xml', 'csv_accounts', false, []],
            [new CsvFile(__DIR__ . '/_data/csv-import/tw_accounts.tabs.csv', "\t"), 'expectation.fullImport.xml', 'csv_accounts', false, []],
            [new CsvFile(__DIR__ . '/_data/csv-import/tw_accounts.tabs.csv', "	"), 'expectation.fullImport.xml', 'csv_accounts', false, []],
            [new CsvFile(__DIR__ . '/_data/csv-import/tw_accounts.extraColumns.csv'), 'expectation.fullImport.xml', 'csv_accounts', false, []],
            [new CsvFile(__DIR__ . '/_data/csv-import/tw_accounts.changedColumnsOrder.csv'), 'expectation.fullImport.xml', 'csv_accounts', false, []],
            [new CsvFile(__DIR__ . '/_data/csv-import/escaping/standard.csv'), 'escaping/expectation.standard.xml', 'csv_2cols', false, []],
            [new CsvFile(__DIR__ . '/_data/csv-import/escaping/raw.csv', "\t", "", "\\"), 'escaping/expectation.standard.xml', 'csv_2cols', false, []],

            // line breaks
            [new CsvFile(__DIR__ . '/_data/csv-import/csv_breaks.win.csv'), 'expectation.fullImport.xml', 'csv_breaks', false, []],
            [new CsvFile(__DIR__ . '/_data/csv-import/csv_breaks.unix.csv'), 'expectation.fullImport.xml', 'csv_breaks', false, []],

            // reserved words
            [new CsvFile(__DIR__ . '/_data/csv-import/reserved-words.csv'), 'expectation.reserved-words.xml', 'table-with-dash', false, []],

            // incremental imports
            [new CsvFile(__DIR__ . '/_data/csv-import/tw_accounts.csv'), 'expectation.incrementalImport.xml', 'csv_accounts', true, []],
            [new CsvFile(__DIR__ . '/_data/csv-import/tw_accounts.tabs.csv', "\t"), 'expectation.incrementalImport.xml', 'csv_accounts', true, []],
            [new CsvFile(__DIR__ . '/_data/csv-import/tw_accounts.tabs.csv', "	"), 'expectation.incrementalImport.xml', 'csv_accounts', true, []],
            [new CsvFile(__DIR__ . '/_data/csv-import/tw_accounts.extraColumns.csv'), 'expectation.incrementalImport.xml', 'csv_accounts', true, []],
            [new CsvFile(__DIR__ . '/_data/csv-import/tw_accounts.changedColumnsOrder.csv'), 'expectation.incrementalImport.xml', 'csv_accounts', true, []],

            // specified columns import
            [new CsvFile(__DIR__ . '/_data/csv-import/tw_accounts.columnImport.csv'), 'expectation.incrementalImportColumnsList.xml', 'csv_accounts',
                true, [], true
            ],
            [new CsvFile(__DIR__ . '/_data/csv-import/tw_accounts.columnImportIsImported.csv'), 'expectation.incrementalImportColumnsListIsImported.xml',
                'csv_accounts', true, [], true
            ]
        ];
    }
}
