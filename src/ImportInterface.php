<?php

declare(strict_types=1);

namespace Keboola\Db\Import;

interface ImportInterface
{
    public function import(string $tableName, array $columns, array $sourceData, array $options = []): Result;

    public function getIncremental(): bool;

    public function setIncremental(bool $incremental): ImportInterface;

    public function getIgnoreLines(): int;

    public function setIgnoreLines(int $linesCount): ImportInterface;
}
