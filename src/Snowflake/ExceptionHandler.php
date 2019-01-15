<?php

declare(strict_types=1);

namespace Keboola\Db\Import\Snowflake;

use Keboola\Db\Import;
use Keboola\Db\Import\MessageTransformation;

class ExceptionHandler
{
    /** @var Import\MessageTransformation[] */
    private $messageTransformations = [];


    public function __construct()
    {
        $this->messageTransformations = [
            new MessageTransformation(
                "/String \'([^\']*)\' is too long .* SQL state 22000/",
                "String '%s' cannot be inserted because it's bigger than column size",
                Import\Exception::ROW_SIZE_TOO_LARGE,
                [1]
            ),
        ];
    }

    public function createException(\Throwable $exception): \Throwable
    {
        foreach ($this->messageTransformations as $messageTransformation) {
            if (preg_match(
                $messageTransformation->getPattern(),
                $exception->getMessage(),
                $matches
            )) {
                return $messageTransformation->getImportException($matches);
            }
        }

        return $exception;
    }
}
