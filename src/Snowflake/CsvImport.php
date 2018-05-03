<?php

declare(strict_types=1);

namespace Keboola\Db\Import\Snowflake;

class CsvImport extends CsvImportBase
{
    protected function importDataToStagingTable(string $stagingTableName, array $columns, array $sourceData): void
    {
        foreach ($sourceData as $csvFile) {
            $this->importTableFromCsv(
                $stagingTableName,
                $csvFile,
                false
            );
        }
    }
}
