<?php

namespace Keboola\Db\Import;

class CsvImportRedshift extends RedshiftBaseCsv
{

    protected function importDataToStagingTable($stagingTableName, $columns, $sourceData)
    {
        foreach ($sourceData as $csvFile) {
            $this->importTable($stagingTableName, $columns, $csvFile, false);
        }
    }

}