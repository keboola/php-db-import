<?php

namespace Keboola\Db\Import;

class CsvManifestImportRedshift extends RedshiftBaseCsv
{

    protected function importDataToStagingTable($stagingTableName, $columns, $sourceData)
    {
        foreach ($sourceData as $csvFile) {
            $this->importTable($stagingTableName, $columns, $csvFile, true);
        }
    }

}