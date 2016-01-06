<?php
/**
 * Integration test
 * User: Martin Halamíček
 * Date: 13.4.12
 * Time: 9:59
 *
 */

namespace Keboola\DbImportTest;

use Keboola\Db\Import\CsvImportMysql,
    Keboola\Csv\CsvFile;

class CsvImportRedshiftTest extends \PHPUnit_Framework_TestCase
{
	/**
	 * @var \PDO
	 */
	protected $connection;

	private $destSchemaName = 'in.c-tests';

	private $sourceSchemaName = 'some.tests';


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
		$commands = array();

		$schemas = array($this->sourceSchemaName, $this->destSchemaName);

		$currentDate = new \DateTime('now', new \DateTimeZone('UTC'));
		$now = $currentDate->format('Ymd H:i:s');

		foreach ($schemas as $schema) {


			$tablesToDelete = array('out.csv_2Cols', 'accounts', 'types', 'names', 'with_ts');
			foreach ($tablesToDelete as $tableToDelete) {
				$stmt = $this->connection
                    ->prepare("SELECT table_name FROM information_schema.tables WHERE table_name = ? AND table_schema = ?");

                if ($stmt->execute(array(strtolower($tableToDelete), strtolower($schema))) && $stmt->fetch()) {
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
		}

		$commands[]= "
			CREATE TABLE \"{$this->sourceSchemaName}\".types (
				  col1  varchar(65535) NOT NULL,
				  col2 boolean NOT NULL
				);
		";

		$commands[]= "
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

		foreach ($commands as $command) {
			$this->connection->query($command);
		}

	}


	/**
	 * @dataProvider tables
	 */
	public function testImport($sourceData, $columns, $expected, $tableName, $type = 'csv')
	{

		$import = $this->getImport($type);
		$import->setIgnoreLines(1);

		$import->import($tableName, $columns, $sourceData);


		$tableColumns = $this->describeTable(strtolower($tableName), strtolower($this->destSchemaName));
		if (!in_array('_timestamp', $columns)) {
			unset($tableColumns['_timestamp']);
		}

		$columnsSql = implode(", ", array_map(function($column) {
            return '"' . $column . '"';
        }, array_keys($tableColumns)));

		$importedData = $this->connection->query("SELECT $columnsSql FROM \"{$this->destSchemaName}\".\"$tableName\"")->fetchAll(\PDO::FETCH_NUM);

		$this->assertArrayEqualsSorted($expected, $importedData, 0);
	}

	/**
	 * @dataProvider tablesIncremental
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
			->import($tableName, $columns, array($initialImportFile));

		$timestampsByIdsAfterFullLoad = [];

        foreach ($this->connection->query("SELECT id, _timestamp FROM \"{$this->destSchemaName}\".\"$tableName\"")->fetchAll() as $row) {
            $timestampsByIdsAfterFullLoad[$row['id']] = $row['_timestamp'];
        }

		sleep(2);
		$import
			->setIncremental(true)
			->import($tableName, $columns, array($incrementFile));

		$tableColumns = $this->describeTable($tableName, strtolower($this->destSchemaName));
		unset($tableColumns['_timestamp']);

		$columnsSql = implode(", ", array_map(function($column) {
			return '"' . $column . '"';
		}, array_keys($tableColumns)));


		$timestampsByIdsAfterIncrement = [];
        foreach ($this->connection->query("SELECT id, _timestamp FROM \"{$this->destSchemaName}\".\"$tableName\"")->fetchAll() as $row) {
            $timestampsByIdsAfterIncrement[$row['id']] = $row['_timestamp'];
        }

		$changedTimestamps = array_diff($timestampsByIdsAfterIncrement, $timestampsByIdsAfterFullLoad);
		$updatedRows = array_keys($changedTimestamps);
		sort($updatedRows);
		sort($rowsShouldBeUpdated);
		$this->assertEquals($rowsShouldBeUpdated, $updatedRows);

		$importedData = $this->connection->query("SELECT $columnsSql FROM \"{$this->destSchemaName}\".\"$tableName\"")->fetchAll();
		$this->assertArrayEqualsSorted($expected, $importedData, 0);
	}


	public function testCopyInvalidParamsShouldThrowException()
	{
		$import = $this->getImport('copy');

		try {
			$import->import('out.csv_2Cols', array('col1', 'col2'), array());
			$this->fail('exception should be thrown');
		} catch (\Keboola\Db\Import\Exception $e) {
			$this->assertEquals(\Keboola\Db\Import\Exception::INVALID_SOURCE_DATA, $e->getCode());
		}
	}


	public function testCopyInvalidSourceDataShouldThrowException()
	{
		$import = $this->getImport('copy');

		try {
			$import->import('out.csv_2Cols', array('c1', 'c2'), array(
				'schemaName' => $this->sourceSchemaName,
				'tableName' => 'names')
			);
			$this->fail('exception should be thrown');
		} catch (\Keboola\Db\Import\Exception $e) {
			$this->assertEquals(\Keboola\Db\Import\Exception::COLUMNS_COUNT_NOT_MATCH, $e->getCode());
		}
	}

	public function testInvalidManifestImport()
	{
		$initialFile =  new \Keboola\Csv\CsvFile(__DIR__ . '/_data/csv-import/tw_accounts.csv');
		$importFile = new Keboola\Csv\CsvFile('s3://keboola-tests/02_tw_accounts.csv.invalid.manifest');

		$import = $this->getImport('manifest');
		$import->setIgnoreLines(1);

		try {
			$import->import('accounts', $initialFile->getHeader(), array($importFile));
		} catch (\Keboola\Db\Import\Exception $e) {
			$this->assertEquals(\Keboola\Db\Import\Exception::MANDATORY_FILE_NOT_FOUND, $e->getCode());
		}

	}

	public function tables()
	{

		$expectedEscaping = array();
		$file =  new \Keboola\Csv\CsvFile(__DIR__ . '/_data/csv-import/escaping/standard-with-enclosures.csv');
		foreach ($file as $row) {
			$expectedEscaping[] = $row;
		}
		$escapingHeader = array_shift($expectedEscaping); // remove header
		$expectedEscaping = array_values($expectedEscaping);


		$expectedAccounts = array();
		$file =  new \Keboola\Csv\CsvFile(__DIR__ . '/_data/csv-import/tw_accounts.csv');
		foreach ($file as $row) {
			$expectedAccounts[] = $row;
		}
		$accountsHeader = array_shift($expectedAccounts); // remove header
		$expectedAccounts = array_values($expectedAccounts);

		$file =  new \Keboola\Csv\CsvFile(__DIR__ . '/_data/csv-import/tw_accounts.changedColumnsOrder.csv');
		$accountChangedColumnsOrderHeader = $file->getHeader();


		return array(

			// full imports
			array(array(new CsvFile('s3://keboola-tests/standard-with-enclosures.csv')), $escapingHeader, $expectedEscaping, 'out.csv_2Cols'),
			array(array(new CsvFile('s3://keboola-tests/standard-with-enclosures.csv.gz')), $escapingHeader, $expectedEscaping, 'out.csv_2Cols'),
			array(array(new CsvFile('s3://keboola-tests/standard-with-enclosures.tabs.csv', "\t")), $escapingHeader, $expectedEscaping, 'out.csv_2Cols'),
			array(array(new CsvFile('s3://keboola-tests/raw.rs.csv', "\t", '', '\\')), $escapingHeader, $expectedEscaping, 'out.csv_2Cols'),
			array(array(new CsvFile('s3://keboola-tests/tw_accounts.changedColumnsOrder.csv')), $accountChangedColumnsOrderHeader, $expectedAccounts, 'accounts'),

			array(array(new CsvFile('s3://keboola-tests/tw_accounts.csv')), $accountsHeader, $expectedAccounts, 'accounts'),
			array(array(new CsvFile('s3://keboola-tests/01_tw_accounts.csv.manifest')), $accountsHeader, $expectedAccounts, 'accounts', 'manifest'),
			array(array(new CsvFile('s3://keboola-tests/03_tw_accounts.csv.gzip.manifest')), $accountsHeader, $expectedAccounts, 'accounts', 'manifest'),

			array(array('schemaName' => $this->sourceSchemaName, 'tableName' => 'out.csv_2Cols'), $escapingHeader, array(array('a', 'b'), array('c', 'd')), 'out.csv_2Cols', 'copy'),
			array(array('schemaName' => $this->sourceSchemaName, 'tableName' => 'types'), $escapingHeader, array(array('c', '1'), array('d', '0')), 'types', 'copy'),

			// increment to empty table
			array(array(new CsvFile('s3://keboola-tests/tw_accounts.csv')), $accountsHeader, $expectedAccounts, 'accounts'),

			// import table with _timestamp columns - used by snapshots
			array(
				array(new CsvFile('s3://keboola-tests/with-ts.csv')),
				array('col1', 'col2', '_timestamp'),
				array(
					array('a', 'b', '2014-11-10 13:12:06'),
					array('c', 'd', '2014-11-10 14:12:06'),
				),
				'out.csv_2Cols'
			),
		);
	}


	public function tablesIncremental()
	{
		$initialFile =  new CsvFile('s3://keboola-tests/tw_accounts.csv');
		$incrementFile = new CsvFile('s3://keboola-tests/tw_accounts.increment.csv');

		$expectationFile = new CsvFile(__DIR__ . '/_data/csv-import/expectation.tw_accounts.increment.csv');
		$expectedRows = array();
		foreach ($expectationFile as $row) {
			$expectedRows[] = $row;
		}
		$columns = array_shift($expectedRows);
		$expectedRows = array_values($expectedRows);

		return array(
			array($initialFile, $incrementFile, $columns, $expectedRows, 'accounts', array(15, 24)),
		);
	}

	public function assertArrayEqualsSorted($expected, $actual, $sortKey, $message = "")
	{
		$comparsion = function($attrLeft, $attrRight) use($sortKey) {
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
					$this->destSchemaName
				);
				break;
			case 'csv':
				return new \Keboola\Db\Import\CsvImportRedshift(
					$this->connection,
                    getenv('AWS_ACCESS_KEY'),
                    getenv('AWS_SECRET_KEY'),
					$this->destSchemaName
				);
			case 'copy';
				return new \Keboola\Db\Import\CopyImportRedshift(
					$this->connection,
					$this->destSchemaName
				);
				break;
			default:
				throw new \Exception("Import type $type not found");

		}
	}

    private  function describeTable($tableName, $schemaName = null)
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
            WHERE a.attnum > 0 AND c.relname = ".$this->connection->quote($tableName);
        if ($schemaName) {
            $sql .= " AND n.nspname = ".$this->connection->quote($schemaName);
        }
        $sql .= ' ORDER BY a.attnum';

        $stmt = $this->connection->query($sql);

        // Use FETCH_NUM so we are not dependent on the CASE attribute of the PDO connection
        $result = $stmt->fetchAll();

        $attnum        = 0;
        $nspname       = 1;
        $relname       = 2;
        $colname       = 3;
        $type          = 4;
        $atttypemod    = 5;
        $complete_type = 6;
        $default_value = 7;
        $notnull       = 8;
        $length        = 9;
        $contype       = 10;
        $conkey        = 11;

        $desc = array();
        foreach ($result as $key => $row) {
            $defaultValue = $row[$default_value];
            if ($row[$type] == 'varchar' || $row[$type] == 'bpchar' ) {
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
            list($primary, $primaryPosition, $identity) = array(false, null, false);
            if ($row[$contype] == 'p') {
                $primary = true;
                $primaryPosition = array_search($row[$attnum], explode(',', $row[$conkey])) + 1;
                $identity = (bool) (preg_match('/^nextval/', $row[$default_value]));
            }
            $desc[$row[$colname]] = array(
                'SCHEMA_NAME'      => $row[$nspname],
                'TABLE_NAME'       => $row[$relname],
                'COLUMN_NAME'      => $row[$colname],
                'COLUMN_POSITION'  => $row[$attnum],
                'DATA_TYPE'        => $row[$type],
                'DEFAULT'          => $defaultValue,
                'NULLABLE'         => (bool) ($row[$notnull] != 't'),
                'LENGTH'           => $row[$length],
                'SCALE'            => null, // @todo
                'PRECISION'        => null, // @todo
                'UNSIGNED'         => null, // @todo
                'PRIMARY'          => $primary,
                'PRIMARY_POSITION' => $primaryPosition,
                'IDENTITY'         => $identity
            );
        }
        return $desc;
    }
}
