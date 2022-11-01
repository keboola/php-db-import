<?php

declare(strict_types=1);

namespace Keboola\Db\Import\Helper;

use Keboola\Csv\CsvReader;

/**
 * CsvReader no longer extends SplFileInfo this class aims to implement used methods in class
 */
class CsvFile extends CsvReader
{
    /**
     * @return string the base name without path information.
     */
    public function getBasename(): string
    {
        return basename($this->fileName);
    }

    /**
     * Gets the path to the file
     *
     * @return string The path to the file.
     */
    public function getPathname(): string
    {
        return $this->fileName;
    }
}
