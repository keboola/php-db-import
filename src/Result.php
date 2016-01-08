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
        return (array) $this->getKeyValue('warnings', array());
    }

    public function getImportedRowsCount()
    {
        return (int) $this->getKeyValue('importedRowsCount');
    }

    public function getImportedColumns()
    {
        return (array) $this->getKeyValue('importedColumns', array());
    }

    public function getTimers()
    {
        return (array) $this->getKeyValue('timers', array());
    }


    private function getKeyValue($keyName, $default = null)
    {
        return isset($this->results[$keyName]) ? $this->results[$keyName] : $default;
    }

}