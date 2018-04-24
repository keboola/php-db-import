<?php
namespace Keboola\Db\Import\Snowflake;

class CsvImport extends CsvImportBase
{
    protected function importDataToStagingTable(string $stagingTableName, array $columns, array $sourceData)
    {
        foreach ($sourceData as $csvFile) {
            $this->importTable(
                $stagingTableName,
                $csvFile,
                [
                    'isManifest' => false,
                ]
            );
        }
    }
}
