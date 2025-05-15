<?php

declare(strict_types=1);

namespace Keboola\Db\Import\Snowflake;

use Keboola\Db\Import\Exception;

abstract class AbstractConnection
{
    /**
     * @param string $sql
     * @param list<mixed>|array<string, mixed> $bind
     */
    abstract public function query(string $sql, array $bind = []): void;

    /**
     * @param string $sql
     * @param array<int|string, mixed> $bind
     * @return list<array<string,string|int|null>>
     */
    abstract public function fetchAll(string $sql, array $bind = []): array;

    /**
     * @param string $sql
     * @param list<mixed>|array<string, mixed> $bind
     * @param callable $callback
     * @return void
     * @throws \Doctrine\DBAL\Exception
     */
    abstract public function fetch(string $sql, array $bind, callable $callback): void;

    abstract public function disconnect(): void;

    public function quoteIdentifier(string $value): string
    {
        $q = '"';
        return ($q . str_replace("$q", "$q$q", $value) . $q);
    }

    /**
     * Returns information about table:
     *  - name
     *  - bytes
     *  - rows
     * @return array<mixed>
     * @throws Exception
     */
    public function describeTable(string $schemaName, string $tableName): array
    {
        $tables = $this->fetchAll(sprintf(
            'SHOW TABLES LIKE %s IN SCHEMA %s',
            "'" . addslashes($tableName) . "'",
            $this->quoteIdentifier($schemaName),
        ));

        foreach ($tables as $table) {
            if ($table['name'] === $tableName) {
                return $table;
            }
        }

        throw new Exception("Table $tableName not found in schema $schemaName");
    }

    /**
     * @param string $schemaName
     * @param string $tableName
     * @return array<array<string, mixed>>
     */
    public function describeTableColumns(string $schemaName, string $tableName): array
    {
        return $this->fetchAll(sprintf(
            'SHOW COLUMNS IN %s.%s',
            $this->quoteIdentifier($schemaName),
            $this->quoteIdentifier($tableName),
        ));
    }

    /**
     * @return list<string>
     */
    public function getTableColumns(string $schemaName, string $tableName): array
    {
        /** @var list<string> $tableColumns */
        $tableColumns = array_map(function ($column) {
            return $column['column_name'];
        }, $this->describeTableColumns($schemaName, $tableName));

        return $tableColumns;
    }

    /**
     * Returns primary key columns for the table
     * @return list<string>
     */
    public function getTablePrimaryKey(string $schemaName, string $tableName): array
    {
        $cols = $this->fetchAll(sprintf(
            'DESC TABLE %s.%s',
            $this->quoteIdentifier($schemaName),
            $this->quoteIdentifier($tableName),
        ));
        /** @var list<string> $pkCols */
        $pkCols = [];
        foreach ($cols as $col) {
            if ($col['primary key'] !== 'Y' || !is_string($col['name'])) {
                continue;
            }
            $pkCols[] = $col['name'];
        }

        return $pkCols;
    }
}
