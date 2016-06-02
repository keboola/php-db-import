<?php

namespace Keboola\Db\Import;

class CsvImportRedshift extends RedshiftBaseCsv
{

    protected function importDataToStagingTable($stagingTempTableName, $columns, $sourceData)
    {
        foreach ($sourceData as $csvFile) {
            $this->importTable($stagingTempTableName, $columns, $csvFile, false);
        }
    }

}