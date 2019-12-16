<?php

declare(strict_types=1);

namespace Keboola\Db\Import\Snowflake;

use Keboola\Db\Import\Snowflake\Exception\ExceptionInterface;
use Keboola\Db\Import\Snowflake\Exception\RuntimeException;
use Keboola\Db\Import\Snowflake\Exception\SnowflakeDbAdapterException;
use Keboola\Db\Import\Snowflake\Exception\StringTooLongException;
use Keboola\Db\Import\Snowflake\Exception\WarehouseTimeoutReached;

class ExceptionHandler
{
    public function handleException(\Throwable $e, ?string $sql = null): void
    {
        $pattern = "/String \'([^\']*)\' is too long .* SQL state 22000/";
        $matches = null;
        if (preg_match($pattern, $e->getMessage(), $matches)) {
            array_shift($matches); // remove the whole string from matches
            throw new StringTooLongException(vsprintf(
                "String '%s' cannot be inserted because it's bigger than column size",
                $matches
            ));
        }

        $pattern = '/Statement reached its statement or warehouse timeout of ([0-9]+) second.* SQL state 57014/';
        $matches = null;
        if (preg_match($pattern, $e->getMessage(), $matches)) {
            array_shift($matches); // remove the whole string from matches
            throw new WarehouseTimeoutReached(vsprintf(
                'Query reached its timeout %d second(s)',
                $matches
            ));
        }

        if ($sql) {
            throw new RuntimeException(
                sprintf('Error "%s" while executing query "%s"', $e->getMessage(), $sql),
                $e->getCode(),
                $e
            );
        }
        if ($e instanceof ExceptionInterface) {
            throw $e;
        }
        throw new SnowflakeDbAdapterException($e->getMessage(), $e->getCode(), $e);
    }
}
