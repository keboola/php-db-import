<?php
/**
 *
 * User: Martin Halamíček
 * Date: 13.4.12
 * Time: 14:31
 *
 */

namespace Keboola\Db\Import;

class Exception extends \Exception
{

	const UNKNOWN_ERROR = 1;
	const TABLE_NOT_EXISTS = 2;
	const COLUMNS_COUNT_NOT_MATCH = 3;
	const INVALID_COLUMN_NAME = 4;
	const DUPLICATE_COLUMN_NAMES = 5;
	const NO_COLUMNS = 6;
	const MANDATORY_FILE_NOT_FOUND = 7;
	const INVALID_SOURCE_DATA = 8;
	const DATA_TYPE_MISMATCH = 9;
	const INVALID_CSV_PARAMS = 10;
}