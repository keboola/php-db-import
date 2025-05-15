<?php

declare(strict_types=1);

namespace Keboola\Db\Import\Snowflake;

use Doctrine\DBAL\Connection as DoctrineConnection;

class ConnectionDBAL extends AbstractConnection
{
    private DoctrineConnection $connection;

    public function __construct(DoctrineConnection $connection)
    {
        $this->connection = $connection;
    }

    public function getConnection(): DoctrineConnection
    {
        return $this->connection;
    }


    public function query(string $sql, array $bind = []): void
    {
        $this->connection->executeQuery($sql, $bind);
    }

    public function fetchAll(string $sql, array $bind = []): array
    {
        // @phpstan-ignore-next-line
        return $this->connection->fetchAllAssociative($sql, $bind);
    }

    public function fetch(string $sql, array $bind, callable $callback): void
    {
        foreach ($this->connection->fetchAllAssociative($sql, $bind) as $row) {
            $callback($row);
        }
    }

    public function disconnect(): void
    {
        $this->connection->close();
    }
}
