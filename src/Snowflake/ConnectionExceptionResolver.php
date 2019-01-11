<?php

declare(strict_types=1);

namespace Keboola\Db\Import\Snowflake;

use Keboola\Db\Import;

class ConnectionExceptionResolver
{
    public static function resolveException(\Throwable $e): void
    {
        if (get_class($e) === \ErrorException::class) {
            static::processErrorException($e);
        }
        throw $e;
    }

    private static function processErrorException(\Throwable $e): void
    {
        if (preg_match('/is too long .* SQL state 22000/', $e->getMessage())) {
            throw new Import\Exception(
                "One of imported column exceeds maximum length: " . $e->getMessage(),
                Import\Exception::ROW_SIZE_TOO_LARGE,
                $e
            );
        }
    }
}
