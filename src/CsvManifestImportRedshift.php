<?php

declare(strict_types=1);

namespace Keboola\Db\Import;

class CsvManifestImportRedshift extends RedshiftBaseCsv
{

    protected function importDataToStagingTable(string $stagingTempTableName, array $columns, array $sourceData, array $options = []): void
    {
        foreach ($sourceData as $csvFile) {
            $this->importTable(
                $stagingTempTableName,
                $columns,
                $csvFile,
                array_merge(
                    [
                        'isManifest' => true,
                    ],
                    $options,
                ),
            );
        }
    }
}
