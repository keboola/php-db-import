<?php

declare(strict_types=1);

namespace Keboola\DbImportTest\Helper;

use Keboola\Db\Import\Helper\TableHelper;
use PHPUnit_Framework_TestCase;

class TableHelperTest extends PHPUnit_Framework_TestCase
{
    public function testGenerateStagingTableName(): void
    {
        $tableName = TableHelper::generateStagingTableName();
        self::assertNotEquals(TableHelper::STAGING_TABLE_PREFIX, $tableName);
        self::assertStringStartsWith(TableHelper::STAGING_TABLE_PREFIX, $tableName);
    }
}
