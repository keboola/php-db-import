<?php

namespace Keboola\Db\Import;

class CsvManifestImportRedshift extends RedshiftBaseCsv
{

    protected function importDataToStagingTable($stagingTempTableName, $columns, $sourceData)
    {
        foreach ($sourceData as $csvFile) {
            $this->importTable($stagingTempTableName, $columns, $csvFile, true);
        }
    }

}