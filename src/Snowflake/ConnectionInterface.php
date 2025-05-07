<?php

declare(strict_types=1);

namespace Keboola\Db\Import\Snowflake;

interface ConnectionInterface
{
    public function query(string $sql, array $bind = []): void;

    public function fetchAll(string $sql, array $bind = []): array;

    public function fetch(string $sql, array $bind, callable $callback): void;

    public function disconnect(): void;
}
