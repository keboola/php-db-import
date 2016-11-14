<?php
/**
 * Created by PhpStorm.
 * User: martinhalamicek
 * Date: 27/04/16
 * Time: 15:59
 */
namespace Keboola\Db\Import\Snowflake;

class CsvManifestImport extends CsvImportBase
{
    protected function importDataToStagingTable($stagingTableName, $columns, $sourceData)
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
