<?php

declare(strict_types=1);

namespace Keboola\Db\Import\Helper;

final class TableHelper
{
    public const STAGING_TABLE_PREFIX = '__temp_csvimport';

    public static function generateStagingTableName(): string
    {
        return str_replace('.', '_', uniqid(self::STAGING_TABLE_PREFIX, true));
    }
}
