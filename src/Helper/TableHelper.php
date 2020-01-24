<?php

declare(strict_types=1);

namespace Keboola\Db\Import\Helper;

final class TableHelper
{
    public static function generateStagingTableName(): string
    {
        return '__temp_' . str_replace('.', '_', uniqid('csvimport', true));
    }
}
