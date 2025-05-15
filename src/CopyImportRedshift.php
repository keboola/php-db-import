<?php

declare(strict_types=1);

namespace Keboola\Db\Import;

use Throwable;
use Tracy\Debugger;

class CopyImportRedshift extends RedshiftBase
{

    protected function importDataToStagingTable(string $stagingTempTableName, array $columns, array $sourceData, array $options = []): void
    {
        if (!isset($sourceData['schemaName'])) {
            throw new Exception('Invalid source data. schemaName must be set', Exception::INVALID_SOURCE_DATA);
        }

        if (!isset($sourceData['tableName'])) {
            throw new Exception('Invalid source data. schemaName must be set', Exception::INVALID_SOURCE_DATA);
        }

        $sourceColumnTypes = $this->describeTable(
            strtolower($sourceData['tableName']),
            strtolower($sourceData['schemaName']),
        );

        $sql = 'INSERT INTO ' . $this->tableNameEscaped($stagingTempTableName) . ' (' . implode(
            ', ',
            array_map(function ($column) {
                return $this->quoteIdentifier($column);
            }, $columns),
        ) . ') ';

        $sql .= 'SELECT ' . implode(',', array_map(function ($column) use ($sourceColumnTypes, $options) {
            if ($sourceColumnTypes[$column]['DATA_TYPE'] === 'bool') {
                return sprintf('DECODE(%s, true, 1, 0) ', $this->quoteIdentifier($column));
            } else {
                if (isset($options['convertEmptyValuesToNull']) && in_array($column, $options['convertEmptyValuesToNull'])) {
                    return "NULLIF(CAST({$this->quoteIdentifier($column)} as varchar), '') ";
                } else {
                    return "COALESCE(CAST({$this->quoteIdentifier($column)} as varchar), '') ";
                }
            }
        }, $columns)) . ' FROM ' . $this->nameWithSchemaEscaped($sourceData['tableName'], $sourceData['schemaName']);

        try {
            Debugger::timer('copyToStaging');
            $this->query($sql);
            $this->addTimer('copyToStaging', Debugger::timer('copyToStaging'));
        } catch (Throwable $e) {
            if (strpos($e->getMessage(), 'Datatype mismatch') !== false) {
                throw new Exception($e->getMessage(), Exception::DATA_TYPE_MISMATCH, $e);
            }
            // everything is user error
            throw new Exception($e->getMessage(), Exception::UNKNOWN_ERROR, $e);
        }
    }
}
