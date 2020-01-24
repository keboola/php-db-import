<?php

declare(strict_types=1);

namespace Keboola\DbImportTest\Helper;

use Keboola\Db\Import\Helper\TableHelper;

class TableHelperTest extends \PHPUnit_Framework_TestCase
{
    public function testGenerateStagingTableName(): void
    {
        $tableName = TableHelper::generateStagingTableName();
        self::assertContains('__temp_csvimport', $tableName);
    }
}
