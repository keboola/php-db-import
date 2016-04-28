<?php
/**
 * Created by PhpStorm.
 * User: martinhalamicek
 * Date: 28/04/16
 * Time: 08:29
 */
namespace Keboola\Db\Import\Snowflake;

use Keboola\Db\Import\Exception;
use Tracy\Debugger;

class CopyImport extends ImportBase
{

    protected function importDataToStagingTable($stagingTableName, $columns, $sourceData)
    {
        if (!isset($sourceData['schemaName'])) {
            throw new Exception('Invalid source data. schemaName must be set', Exception::INVALID_SOURCE_DATA);
        }

        if (!isset($sourceData['tableName'])) {
            throw new Exception('Invalid source data. schemaName must be set', Exception::INVALID_SOURCE_DATA);
        }

        $sql = "INSERT INTO " . $this->nameWithSchemaEscaped($stagingTableName) . " (" . implode(', ', array_map(function ($column) {
                return $this->quoteIdentifier($column);
            }, $columns)) . ") ";

        $sql .= "SELECT " . implode(',', array_map(function ($column) {
                return $this->quoteIdentifier($column);
            }, $columns)) . " FROM " . $this->nameWithSchemaEscaped($sourceData['tableName'], $sourceData['schemaName']);

        try {
            Debugger::timer('copyToStaging');
            $this->query($sql);
            $this->addTimer('copyToStaging', Debugger::timer('copyToStaging'));
        } catch (\Exception $e) {
            // everything is user error
            throw new Exception($e->getMessage(), Exception::UNKNOWN_ERROR, $e);
        }
    }
}