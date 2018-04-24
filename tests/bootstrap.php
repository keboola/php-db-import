<?php

declare(strict_types=1);

date_default_timezone_set('Europe/Prague');
ini_set('display_errors', '1');
error_reporting(E_ALL);

set_error_handler(function ($errno, $errstr, $errfile, $errline, array $errcontext) {
    throw new \ErrorException($errstr, 0, $errno, $errfile, $errline);
});

require_once 'vendor/autoload.php';
