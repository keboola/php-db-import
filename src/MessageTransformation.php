<?php

declare(strict_types=1);

namespace Keboola\Db\Import;

use Keboola\Db\Import\Exception as ImportException;

class MessageTransformation
{
    private string $pattern;

    private string $message;

    private int $errorCode;

    /** @var array */
    private array $argumentIndexes;

    public function __construct(string $pattern, string $message, int $errorCode, array $argumentIndexes = [])
    {
        $this->pattern = $pattern;
        $this->message = $message;
        $this->errorCode = $errorCode;
        $this->argumentIndexes = $argumentIndexes;
    }

    public function getPattern(): string
    {
        return $this->pattern;
    }

    public function getImportException(array $arguments = []): ImportException
    {
        $args = [];
        foreach ($this->argumentIndexes as $argumentIndex) {
            $args[] = $arguments[$argumentIndex];
        }

        return new ImportException(vsprintf($this->message, $args), $this->errorCode);
    }
}
