<?php

declare(strict_types=1);

namespace Keboola\Db\Import\Snowflake;

use Keboola\Db\Import\Exception;
use Tracy\Debugger;

class CopyImport extends ImportBase
{

    protected function importDataToStagingTable(string $stagingTableName, array $columns, array $sourceData): void
    {
        if (!isset($sourceData['schemaName'])) {
            throw new Exception('Invalid source data. schemaName must be set', Exception::INVALID_SOURCE_DATA);
        }

        if (!isset($sourceData['tableName'])) {
            throw new Exception('Invalid source data. schemaName must be set', Exception::INVALID_SOURCE_DATA);
        }

        $sql = "INSERT INTO " . $this->nameWithSchemaEscaped($stagingTableName) . " (" . implode(
            ', ',
            array_map(function ($column) {
                return $this->quoteIdentifier($column);
            }, $columns)
        ) . ") ";

        $sql .= "SELECT " . implode(',', array_map(function ($column) {
            return $this->quoteIdentifier($column);
        }, $columns)) . " FROM " . $this->nameWithSchemaEscaped($sourceData['tableName'], $sourceData['schemaName']);

        try {
            Debugger::timer('copyToStaging');
            $this->connection->query($sql);
            $rows = $this->connection->fetchAll(sprintf(
                'SELECT COUNT(*) as "count" from %s.%s',
                $this->connection->quoteIdentifier($this->schemaName),
                $this->connection->quoteIdentifier($stagingTableName)
            ));
            $this->importedRowsCount += (int) $rows[0]['count'];

            $this->addTimer('copyToStaging', Debugger::timer('copyToStaging'));
        } catch (\Throwable $e) {
            // everything is user error
            throw new Exception($e->getMessage(), Exception::UNKNOWN_ERROR, $e);
        }
    }
}
