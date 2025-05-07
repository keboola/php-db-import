<?php

declare(strict_types=1);

namespace Keboola\Db\Import;

class Result
{
    private array $results;

    public function __construct(array $results)
    {
        $this->results = $results;
    }

    public function getWarnings(): array
    {
        return (array) $this->getKeyValue('warnings', []);
    }

    public function getImportedRowsCount(): int
    {
        return (int) $this->getKeyValue('importedRowsCount');
    }

    public function getImportedColumns(): array
    {
        return (array) $this->getKeyValue('importedColumns', []);
    }

    public function getTimers(): array
    {
        return (array) $this->getKeyValue('timers', []);
    }

    /**
     * @param mixed|null $default
     * @return mixed
     */
    public function getKeyValue(string $keyName, $default = null)
    {
        return isset($this->results[$keyName]) ? $this->results[$keyName] : $default;
    }
}
