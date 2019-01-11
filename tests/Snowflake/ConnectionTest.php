<?php

declare(strict_types=1);

namespace Keboola\DbImportTest\Snowflake;

use Keboola\Db\Import;
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

    public function testTooLargeColumnInsert(): void
    {
        $connection = $this->createConnection();
        $destSchemaName = 'test';
        $this->prepareSchema($connection, $destSchemaName);
        $size = 10;
        $connection->query(
            sprintf(
                'CREATE TABLE "%s"."%s" ("col1" varchar(%d));',
                $destSchemaName,
                "TEST",
                $size
            )
        );

        $this->expectException(Import\Exception::class);
        $this->expectExceptionMessageRegExp('/column exceeds maximum length/');
        $connection->query(
            sprintf(
                'INSERT INTO "%s"."%s" VALUES(\'%s\');',
                $destSchemaName,
                "TEST",
                implode('', array_fill(0, $size + 1, 'x'))
            )
        );
    }

    private function prepareSchema(Connection $connection, string $schemaName): void
    {
        $connection->query(sprintf('DROP SCHEMA IF EXISTS "%s"', $schemaName));
        $connection->query(sprintf('CREATE SCHEMA "%s"', $schemaName));
    }

    private function createConnection(): Connection
    {
        return new Connection([
            'host' => getenv('SNOWFLAKE_HOST'),
            'port' => getenv('SNOWFLAKE_PORT'),
            'database' => getenv('SNOWFLAKE_DATABASE'),
            'warehouse' => getenv('SNOWFLAKE_WAREHOUSE'),
            'user' => getenv('SNOWFLAKE_USER'),
            'password' => getenv('SNOWFLAKE_PASSWORD'),
        ]);
    }
}
