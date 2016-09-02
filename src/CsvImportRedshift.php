<?php

namespace Keboola\Db\Import;

class CsvImportRedshift extends RedshiftBaseCsv
{

    protected function importDataToStagingTable($stagingTempTableName, $columns, $sourceData, array $options = [])
    {
        foreach ($sourceData as $csvFile) {
            $this->importTable(
                $stagingTempTableName,
                $columns,
                $csvFile,
                array_merge(
                    [
                        'isManifest' => false,
                    ],
                    $options
                )
            );
        }
    }
}
