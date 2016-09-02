<?php

namespace Keboola\Db\Import;

use Keboola\Csv\CsvFile;

interface ImportInterface
{
    /**
     * @param $tableName
     * @param $columns
     * @param array CsvFile $csvFiles
     * @return Result
     */
    public function import($tableName, $columns, array $sourceData, array $options = []);

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
