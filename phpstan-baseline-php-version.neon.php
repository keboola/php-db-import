<?php

declare(strict_types=1);

$includes = [];
if (PHP_VERSION_ID < 80000) {
    $includes[] = __DIR__ . '/phpstan-baseline_v7.neon';
}
if (PHP_VERSION_ID >= 80000) {
    $includes[] = __DIR__ . '/phpstan-baseline.neon';
}

$config = [];
$config['includes'] = $includes;
$config['parameters']['phpVersion'] = PHP_VERSION_ID;

return $config;
