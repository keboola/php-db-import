<?php

declare(strict_types=1);

namespace Keboola\Db\Import;

class CsvManifestImportRedshift extends RedshiftBaseCsv
{

    protected function importDataToStagingTable(string $stagingTempTableName, array $columns, array $sourceData, array $options = []): void
    {
        foreach ($sourceData as $source) {
            $this->importTable(
                $stagingTempTableName,
                $columns,
                $source['file'],
                $source['csvOptions'],
                array_merge(
                    [
                        'isManifest' => true,
                    ],
                    $options
                )
            );
        }
    }
}
