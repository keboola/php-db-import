<?php
/**
 * Created by JetBrains PhpStorm.
 * User: martinhalamicek
 * Date: 06/06/14
 * Time: 10:49
 * To change this template use File | Settings | File Templates.
 */

namespace Keboola\Db\Import;

use Keboola\Csv\CsvFile;



class CsvImportRedshift extends RedshiftBaseCsv
{


	protected function importDataToStagingTable($stagingTableName, $columns, $sourceData)
	{
		foreach ($sourceData as $csvFile) {
			$this->importTable($stagingTableName, $columns, $csvFile, false);
		}
	}

}