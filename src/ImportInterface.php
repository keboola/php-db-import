<?php
/**
 * Import CSV to Mysql database table
 *
 * CSV must contain header with column names
 * CSV columns are matched to database table column names by name
 *
 * All columns in database table must be present in csv except of internal columns begining with underscore
 * character!
 *
 * Static values import:
 *   You can load static values into specified columns for each row
 * 	 Example:
 *   $import->setStaticValues(array('storageApiTransaction' => 45646513))
 * 		->import('orders', $csvFile);
 *
 *
 *
 * User: Martin Halamíček
 * Date: 12.4.12
 * Time: 15:24
 *
 */

namespace Keboola\Db\Import;

use	Keboola\Csv\CsvFile;


interface ImportInterface
{
	/**
	 * @param $tableName
	 * @param $columns
	 * @param array CsvFile $csvFiles
	 * @return Result
	 */
	public function import($tableName, $columns,  array $sourceData);


	public function getIncremental();

	/**
	 * @param $incremental
	 * @return $this
	 */
	public function setIncremental($incremental);

	public function getIgnoreLines();

	/**
	 * @param $linesCount
	 * @return $this
	 */
	public function setIgnoreLines($linesCount);

}
