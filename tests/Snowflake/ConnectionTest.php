<?php

declare(strict_types=1);

namespace Keboola\DbImportTest\Snowflake;

use Keboola\Db\Import;
use Keboola\Db\Import\Snowflake\Connection;
use PHPUnit\Framework\TestCase;

class ConnectionTest extends \PHPUnit\Framework\TestCase
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
                'TEST',
                $size
            )
        );

        $this->expectException(Import\Exception::class);
        $this->expectExceptionMessageMatches('/cannot be inserted because it\'s bigger than column size/');
        $this->expectExceptionCode(Import\Exception::ROW_SIZE_TOO_LARGE);
        $connection->query(
            sprintf(
                'INSERT INTO "%s"."%s" VALUES(\'%s\');',
                $destSchemaName,
                'TEST',
                implode('', array_fill(0, $size + 1, 'x'))
            )
        );
    }

    public function testQueryTimeoutLimit(): void
    {
        $connection = $this->createConnection();
        $connection->query('ALTER SESSION SET STATEMENT_TIMEOUT_IN_SECONDS = 3');

        try {
            $connection->fetchAll('CALL system$wait(5)');
        } catch (Import\Exception $e) {
            $this->assertSame(Import\Exception::class, get_class($e));
            $this->assertRegExp('~timeout~', $e->getMessage());
            $this->assertSame(Import\Exception::QUERY_TIMEOUT, $e->getCode());
        } finally {
            $connection->query('ALTER SESSION UNSET STATEMENT_TIMEOUT_IN_SECONDS');
        }
    }

    public function testQueryTagging(): void
    {
        $connection = new Connection([
            'host' => getenv('SNOWFLAKE_HOST'),
            'port' => getenv('SNOWFLAKE_PORT'),
            'database' => getenv('SNOWFLAKE_DATABASE'),
            'warehouse' => getenv('SNOWFLAKE_WAREHOUSE'),
            'user' => getenv('SNOWFLAKE_USER'),
            'password' => getenv('SNOWFLAKE_PASSWORD'),
            'runId' => 'myRunId',
        ]);

        $destSchemaName = 'testQueryTagging';
        $this->prepareSchema($connection, $destSchemaName);

        $connection->query('CREATE TABLE "' . $destSchemaName . '" ("col1" NUMBER);');

        $queries = $connection->fetchAll(
            '
                SELECT 
                    QUERY_TEXT, QUERY_TAG 
                FROM 
                    TABLE(INFORMATION_SCHEMA.QUERY_HISTORY_BY_SESSION())
                WHERE QUERY_TEXT = \'CREATE TABLE "' . $destSchemaName . '" ("col1" NUMBER);\' 
                ORDER BY START_TIME DESC 
                LIMIT 1
            '
        );

        $this->assertEquals('{"runId":"myRunId"}', $queries[0]['QUERY_TAG']);
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
