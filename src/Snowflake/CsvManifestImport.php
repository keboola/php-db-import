<?php

declare(strict_types=1);

namespace Keboola\Db\Import\Snowflake;

class CsvManifestImport extends CsvImportBase
{
    protected function importDataToStagingTable(string $stagingTableName, array $columns, array $sourceData)
    {
        foreach ($sourceData as $csvFile) {
            $this->importTable(
                $stagingTableName,
                $csvFile,
                [
                    'isManifest' => true,
                ]
            );
        }
    }
}
