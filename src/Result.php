<?php

namespace Keboola\Db\Import;

class Result
{

    private $results;

    public function __construct(array $results)
    {
        $this->results = $results;
    }

    public function getWarnings()
    {
        return (array) $this->getKeyValue('warnings', []);
    }

    public function getImportedRowsCount()
    {
        return (int) $this->getKeyValue('importedRowsCount');
    }

    public function getImportedColumns()
    {
        return (array) $this->getKeyValue('importedColumns', []);
    }

    public function getTimers()
    {
        return (array) $this->getKeyValue('timers', []);
    }


    private function getKeyValue($keyName, $default = null)
    {
        return isset($this->results[$keyName]) ? $this->results[$keyName] : $default;
    }

}