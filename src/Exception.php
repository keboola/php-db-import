<?php

declare(strict_types=1);

namespace Keboola\Db\Import;

class Exception extends \Exception
{
    public const UNKNOWN_ERROR = 1;
    public const TABLE_NOT_EXISTS = 2;
    public const COLUMNS_COUNT_NOT_MATCH = 3;
    public const INVALID_COLUMN_NAME = 4;
    public const DUPLICATE_COLUMN_NAMES = 5;
    public const NO_COLUMNS = 6;
    public const MANDATORY_FILE_NOT_FOUND = 7;
    public const INVALID_SOURCE_DATA = 8;
    public const DATA_TYPE_MISMATCH = 9;
    public const INVALID_CSV_PARAMS = 10;
    public const ROW_SIZE_TOO_LARGE = 11;
    public const QUERY_TIMEOUT = 12;
    public const VALUE_CONVERSION = 13;
}
