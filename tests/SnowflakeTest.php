<?php

namespace Keboola\DbImportTest;

use Keboola\Csv\CsvFile;

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
//        $this->initData();
    }

    private function initData()
    {
        $commands = [];

        $schemas = [$this->sourceSchemaName, $this->destSchemaName];

        $currentDate = new \DateTime('now', new \DateTimeZone('UTC'));
        $now = $currentDate->format('Ymd H:i:s');

        foreach ($schemas as $schema) {


            $tablesToDelete = ['out.csv_2Cols', 'accounts', 'types', 'names', 'with_ts', 'table'];
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

        foreach ($commands as $command) {
            $this->connection->query($command);
        }

    }


    public function testImport()
    {


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
                    $this->destSchemaName
                );
                break;
            case 'csv':
                return new \Keboola\Db\Import\CsvImportRedshift(
                    $this->connection,
                    getenv('AWS_ACCESS_KEY'),
                    getenv('AWS_SECRET_KEY'),
                    getenv('AWS_REGION'),
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
