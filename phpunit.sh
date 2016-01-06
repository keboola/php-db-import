#!/usr/bin/env bash

composer install -n

./vendor/bin/phpunit --verbose --debug tests/CsvImportRedshiftTest.php