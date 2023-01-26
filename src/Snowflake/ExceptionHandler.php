<?php

declare(strict_types=1);

namespace Keboola\Db\Import\Snowflake;

use Keboola\Db\Import;
use Keboola\Db\Import\MessageTransformation;
use Throwable;

class ExceptionHandler
{
    /** @var Import\MessageTransformation[] */
    private array $messageTransformations = [];


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

        $this->messageTransformations[] =
            new MessageTransformation(
                '/Statement reached its statement or warehouse timeout of ([0-9]+) second.* SQL state 57014/',
                'Query reached its timeout %d second(s)',
                Import\Exception::QUERY_TIMEOUT,
                [1]
            );
    }

    /**
     * @return \Throwable|Import\Exception
     */
    public function createException(Throwable $exception): Throwable
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
