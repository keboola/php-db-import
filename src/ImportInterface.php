<?php

declare(strict_types=1);

namespace Keboola\Db\Import;

interface ImportInterface
{
    public function import(string $tableName, array $columns, array $sourceData, array $options = []);

    public function getIncremental();

    public function setIncremental(bool $incremental);

    public function getIgnoreLines();

    public function setIgnoreLines(int $linesCount);
}
