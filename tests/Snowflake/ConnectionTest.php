<?php

declare(strict_types=1);

namespace Keboola\DbImportTest\Snowflake;

use Keboola\Csv\CsvFile;
use Keboola\Db\Import\Exception;
use Keboola\Db\Import\Snowflake\Connection;

class ConnectionTest extends \PHPUnit_Framework_TestCase
{

    public function testConnectionWithoutDbAndWarehouse(): void
    {
        $connection = new Connection([
            'host' => getenv('SNOWFLAKE_HOST'),
            'port' => getenv('SNOWFLAKE_PORT'),
            'user' => getenv('SNOWFLAKE_USER'),
            'password' => getenv('SNOWFLAKE_PASSWORD'),
        ]);

        $databases = $connection->fetchAll('SHOW DATABASES');
        $this->assertNotEmpty($databases);
    }

    public function testConnectionBinding(): void
    {
        $connection = $this->createConnection();
        $destSchemaName = 'test';
        $this->prepareSchema($connection, $destSchemaName);

        $connection->query('CREATE TABLE "' . $destSchemaName . '"."Test" (col1 varchar, col2 varchar)');
        $connection->query('INSERT INTO "' . $destSchemaName . '"."Test" VALUES (\'\\\'a\\\'\',\'b\')');
        $connection->query('INSERT INTO "' . $destSchemaName . '"."Test" VALUES (\'a\',\'b\')');

        $rows = $connection->fetchAll('SELECT * FROM "' . $destSchemaName . '"."Test" WHERE col1 = ?', ["'a'"]);
        $this->assertEmpty($rows);
    }

    public function testConnectionWithDefaultDbAndWarehouse(): void
    {
        $connection = $this->createConnection();
        $destSchemaName = 'test';
        $this->prepareSchema($connection, $destSchemaName);

        // test that we are able to create and query tables
        $connection->query('CREATE TABLE "' . $destSchemaName . '"."TEST" (col1 varchar, col2 varchar)');
        $connection->query('ALTER TABLE "' . $destSchemaName . '"."TEST" DROP COLUMN col2');
        $connection->query('DROP TABLE "' . $destSchemaName . '"."TEST" RESTRICT');
    }

    public function testConnectionEncoding(): void
    {
        $connection = $this->createConnection();

        $destSchemaName = 'test';
        $this->prepareSchema($connection, $destSchemaName);

        $connection->query('CREATE TABLE "' . $destSchemaName . '"."TEST" (col1 varchar, col2 varchar)');
        $connection->query('INSERT INTO  "' . $destSchemaName . '"."TEST" VALUES (\'šperky.cz\', \'módní doplňky.cz\')');

        $data = $connection->fetchAll('SELECT * FROM "' . $destSchemaName . '"."TEST"');

        $this->assertEquals([
            [
                'COL1' => 'šperky.cz',
                'COL2' => 'módní doplňky.cz',
            ],
        ], $data);
    }

    private function prepareSchema(Connection $connection, string $schemaName): void
    {
        $connection->query(sprintf('DROP SCHEMA IF EXISTS "%s"', $schemaName));
        $connection->query(sprintf('CREATE SCHEMA "%s"', $schemaName));
    }

    private function createConnection(): Connection
    {
        $password = getenv('SNOWFLAKE_PASSWORD');

        $passwordCheck = strpos($password, ';') !== false &&
            strpos($password, '{') !== false &&
            strpos($password, '}') !== false
        ;
        $this->assertTrue($passwordCheck, 'Snowflake password must contain semicolon and curly brackets');

        return new Connection([
            'host' => getenv('SNOWFLAKE_HOST'),
            'port' => getenv('SNOWFLAKE_PORT'),
            'database' => getenv('SNOWFLAKE_DATABASE'),
            'warehouse' => getenv('SNOWFLAKE_WAREHOUSE'),
            'user' => getenv('SNOWFLAKE_USER'),
            'password' => $password,
        ]);
    }
}
