<?php

declare(strict_types=1);

namespace Keboola\Db\Import\Snowflake;

use Keboola\Db\Import\Exception;

trait ConnectionTrait
{
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
     * @return array
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

    public function describeTableColumns(string $schemaName, string $tableName): array
    {
        return $this->fetchAll(sprintf(
            'SHOW COLUMNS IN %s.%s',
            $this->quoteIdentifier($schemaName),
            $this->quoteIdentifier($tableName),
        ));
    }

    public function getTableColumns(string $schemaName, string $tableName): array
    {
        return array_map(function ($column) {
            return $column['column_name'];
        }, $this->describeTableColumns($schemaName, $tableName));
    }

    public function getTablePrimaryKey(string $schemaName, string $tableName): array
    {
        $cols = $this->fetchAll(sprintf(
            'DESC TABLE %s.%s',
            $this->quoteIdentifier($schemaName),
            $this->quoteIdentifier($tableName),
        ));
        $pkCols = [];
        foreach ($cols as $col) {
            if ($col['primary key'] !== 'Y') {
                continue;
            }
            $pkCols[] = $col['name'];
        }

        return $pkCols;
    }
}
