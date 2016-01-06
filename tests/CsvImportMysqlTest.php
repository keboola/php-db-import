<?php
/**
 * Integration test
 * User: Martin Halamíček
 * Date: 13.4.12
 * Time: 9:59
 *
 */

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
            sprintf('mysql:host=%s;dbname=%s', getenv('MYSQL_PORT_3306_TCP_ADDR'), getenv('MYSQL_ENV_MYSQL_DATABASE')),
            'root',
            getenv('MYSQL_ENV_MYSQL_ROOT_PASSWORD'),
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
			->import($tableName, $csvFile->getHeader(), array($csvFile));

		$expectedDataset = $this->createMySQLXMLDataSet(__DIR__ . "/_data/csv-import/$expectationsFile");
		$currentDataset = $this->getConnection()->createDataSet();

		$expectedDataset = new \PHPUnit_Extensions_Database_DataSet_DataSetFilter($expectedDataset, array($tableName));
		$expectedDataset->setExcludeColumnsForTable($tableName, array('timestamp'));

		$currentDataset = new \PHPUnit_Extensions_Database_DataSet_DataSetFilter($currentDataset, array($tableName));
		$currentDataset->setExcludeColumnsForTable($tableName, array('timestamp'));

		$this->assertTablesEqual($expectedDataset->getTable($tableName), $currentDataset->getTable($tableName));
		$this->assertEmpty($result->getWarnings());
	}

	public function testImportWithoutHeaders()
	{
		$tableName = 'csv_2cols';
		$result = $this->import
			->setIgnoreLines(0)
			->import($tableName, array('col1', 'col2'), array(new CsvFile(__DIR__ . '/_data/csv-import/escaping/raw-without-headers.csv', "\t", "", "\\")));

		$expectedDataset = $this->createMySQLXMLDataSet(__DIR__ . "/_data/csv-import/escaping/expectation.standard.xml");
		$currentDataset = $this->getConnection()->createDataSet();

		$expectedDataset = new \PHPUnit_Extensions_Database_DataSet_DataSetFilter($expectedDataset, array($tableName));
		$expectedDataset->setExcludeColumnsForTable($tableName, array('timestamp'));

		$currentDataset = new \PHPUnit_Extensions_Database_DataSet_DataSetFilter($currentDataset, array($tableName));
		$currentDataset->setExcludeColumnsForTable($tableName, array('timestamp'));

		$this->assertTablesEqual($expectedDataset->getTable($tableName), $currentDataset->getTable($tableName));
		$this->assertEmpty($result->getWarnings());
	}

	public function testMultipleFilesImport()
	{
		$csvFile = new CsvFile(__DIR__ . '/_data/csv-import/escaping/raw.csv', "\t", "", "\\");
		$importFiles = array(
			$csvFile,
			$csvFile,
		);
		$tableName = 'csv_2cols';
		$result = $this->import
			->setIgnoreLines(1)
			->import($tableName, array('col1', 'col2'), $importFiles);

		$this->assertEquals(7, $result->getImportedRowsCount());
	}

	public function _testImportWithWarnings()
	{
		$importFiles = array(
			new CsvFile(__DIR__ . '/_data/csv-import/escaping/raw-warnings.csv', "\t", "", "\\"),
			new CsvFile(__DIR__ . '/_data/csv-import/escaping/standard.csv'),
		);
		$tableName = 'csv_2cols';
		$result = $this->import
			->setIgnoreLines(1)
			->import($tableName, array('col1', 'col2'), $importFiles);

		$this->assertEquals(7, $result->getImportedRowsCount());
		$this->assertCount(1, $result->getWarnings());
	}


	public function duplicateColumnsData()
	{
		return array(
			array('tw_accounts.duplicateColumnsAdded.csv'),
			array('tw_accounts.duplicateColumnsAdded2.csv'),
		);
	}


	public function testInvalidTableImportShouldThrowException()
	{
		$csvFile = new CsvFile(__DIR__ . "/_data/csv-import/tw_accounts.csv");
		$this->setExpectedException("Keboola\Db\Import\Exception", '', \Keboola\Db\Import\Exception::TABLE_NOT_EXISTS);
		$this->import
			->setIncremental(true)
			->setIgnoreLines(1)
			->import('tw_something', $csvFile->getHeader(), array($csvFile));
	}

	public function testEmptyFileShouldThrowsException()
	{
		$csvFile = new CsvFile(__DIR__ . "/_data/csv-import/empty.csv");
		$this->setExpectedException("Keboola\Db\Import\Exception", '', \Keboola\Db\Import\Exception::NO_COLUMNS);
		$this->import
			->setIgnoreLines(1)
			->import('csv_accounts', $csvFile->getHeader(), array($csvFile));
	}

	public function testEmptyFilePartialShouldThrowsException()
	{
		$csvFile = new CsvFile(__DIR__ . "/_data/csv-import/empty.csv");
		$this->setExpectedException("Keboola\Db\Import\Exception", '', \Keboola\Db\Import\Exception::NO_COLUMNS);
		$this->import
			->import('csv_accounts', $csvFile->getHeader(), array($csvFile));
	}

	public function tables()
	{
		return array(

			// full imports
			array(new CsvFile(__DIR__ . '/_data/csv-import/tw_accounts.csv'), 'expectation.fullImport.xml', 'csv_accounts', false, array()),
			array(new CsvFile(__DIR__ . '/_data/csv-import/tw_accounts.nl-last-row.csv'), 'expectation.fullImport.xml', 'csv_accounts', false, array()),
			array(new CsvFile(__DIR__ . '/_data/csv-import/tw_accounts.tabs.csv', "\t"), 'expectation.fullImport.xml', 'csv_accounts', false, array()),
			array(new CsvFile(__DIR__ . '/_data/csv-import/tw_accounts.tabs.csv', "	"), 'expectation.fullImport.xml', 'csv_accounts', false, array()),
			array(new CsvFile(__DIR__ . '/_data/csv-import/tw_accounts.extraColumns.csv'), 'expectation.fullImport.xml', 'csv_accounts', false, array()),
			array(new CsvFile(__DIR__ . '/_data/csv-import/tw_accounts.changedColumnsOrder.csv'), 'expectation.fullImport.xml', 'csv_accounts', false, array()),
			array(new CsvFile(__DIR__ . '/_data/csv-import/escaping/standard.csv'), 'escaping/expectation.standard.xml', 'csv_2cols', false, array()),
			array(new CsvFile(__DIR__ . '/_data/csv-import/escaping/raw.csv', "\t", "", "\\"), 'escaping/expectation.standard.xml', 'csv_2cols', false, array()),

			// line breaks
			array(new CsvFile(__DIR__ . '/_data/csv-import/csv_breaks.win.csv'), 'expectation.fullImport.xml', 'csv_breaks', false, array()),
			array(new CsvFile(__DIR__ . '/_data/csv-import/csv_breaks.unix.csv'), 'expectation.fullImport.xml', 'csv_breaks', false, array()),

			// incremental imports
			array(new CsvFile(__DIR__ . '/_data/csv-import/tw_accounts.csv'), 'expectation.incrementalImport.xml', 'csv_accounts', true, array()),
			array(new CsvFile(__DIR__ . '/_data/csv-import/tw_accounts.tabs.csv', "\t"), 'expectation.incrementalImport.xml', 'csv_accounts', true, array()),
			array(new CsvFile(__DIR__ . '/_data/csv-import/tw_accounts.tabs.csv', "	"), 'expectation.incrementalImport.xml', 'csv_accounts', true, array()),
			array(new CsvFile(__DIR__ . '/_data/csv-import/tw_accounts.extraColumns.csv'), 'expectation.incrementalImport.xml', 'csv_accounts', true, array()),
			array(new CsvFile(__DIR__ . '/_data/csv-import/tw_accounts.changedColumnsOrder.csv'), 'expectation.incrementalImport.xml', 'csv_accounts', true, array()),

			// specified columns import
			array(new CsvFile(__DIR__ . '/_data/csv-import/tw_accounts.columnImport.csv'), 'expectation.incrementalImportColumnsList.xml', 'csv_accounts',
					true,  array(), true
			),
			array(new CsvFile(__DIR__ . '/_data/csv-import/tw_accounts.columnImportIsImported.csv'), 'expectation.incrementalImportColumnsListIsImported.xml',
				'csv_accounts', true, array(), true
			),
		);
	}

}
