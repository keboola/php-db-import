<?php

declare(strict_types=1);

namespace Keboola\DbImportTest;

use Exception;
use PDO;

class Helpers
{
    /**
     * Import SQL dump from file - extreme fast.
     * Stolen from Nette https://github.com/nette/nette/blob/master/Nette/Database/Helpers.php
     * @return int  count of commands
     */
    public static function loadFromFile(PDO $connection, string $file): int
    {
        @set_time_limit(0); // intentionally @

        $handle = @fopen($file, 'r'); // intentionally @
        if (!$handle) {
            throw new Exception("Cannot open file '$file'.");
        }

        $count = 0;
        $sql = '';
        while (($s = fgets($handle)) !== false) {
            $sql .= $s;
            if (substr(rtrim($s), -1) === ';') {
                $connection->exec($sql); // native query without logging
                $sql = '';
                $count++;
            }
        }
        if (trim($sql) !== '') {
            $connection->exec($sql);
            $count++;
        }
        fclose($handle);
        return $count;
    }
}
