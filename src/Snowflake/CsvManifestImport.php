<?php

declare(strict_types=1);

namespace Keboola\Db\Import\Snowflake;

class CsvManifestImport extends CsvImportBase
{
    protected function importDataToStagingTable(string $stagingTableName, array $columns, array $sourceData): void
    {
        foreach ($sourceData as $source) {
            $this->importTableFromCsv(
                $stagingTableName,
                $source['file'],
                $source['csvOptions'],
                true
            );
        }
    }
}
