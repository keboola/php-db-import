<?php

declare(strict_types=1);

namespace Keboola\Db\Import;

use Aws\S3\S3Client;
use Keboola\Csv\CsvFile;
use PDO;
use Throwable;
use Tracy\Debugger;

abstract class RedshiftBaseCsv extends RedshiftBase
{
    private string $s3key;

    private string $s3secret;

    private string $s3region;

    public function __construct(
        PDO $connection,
        string $s3key,
        string $s3secret,
        string $s3region,
        string $schemaName,
    ) {
        parent::__construct($connection, $schemaName);
        $this->s3key = $s3key;
        $this->s3secret = $s3secret;
        $this->s3region = $s3region;
    }

    /**
     * @param array $columns
     * @param array $options
     *  - isManifest
     *  - copyOptions
     */
    protected function importTable(string $tempTableName, array $columns, CsvFile $csvFile, array $options): void
    {
        if ($csvFile->getEnclosure() && $csvFile->getEscapedBy()) {
            throw new Exception(
                'Invalid CSV params. Either enclosure or escapedBy must be specified for Redshift backend but not both.',
                Exception::INVALID_CSV_PARAMS,
                null,
            );
        }

        try {
            Debugger::timer('copyToStaging');
            $copyOptions = [
                'isManifest' => isset($options['isManifest']) ? $options['isManifest'] : false,
                'copyOptions' => isset($options['copyOptions']) ? $options['copyOptions'] : [],
            ];

            if (isset($options['isManifest']) && $options['isManifest']) {
                $manifest = $this->downloadManifest($csvFile->getPathname());

                // empty manifest handling - do nothing
                if (!count($manifest['entries'])) {
                    $this->addTimer('copyToStaging', Debugger::timer('copyToStaging'));
                    return;
                }

                $copyOptions['isGzipped'] = $this->isGzipped(reset($manifest['entries'])['url']);
            } else {
                $copyOptions['isGzipped'] = $this->isGzipped($csvFile->getPathname());
            }

            $this->query($this->generateCopyCommand($tempTableName, $columns, $csvFile, $copyOptions));
            $this->addTimer('copyToStaging', Debugger::timer('copyToStaging'));
        } catch (Throwable $e) {
            $result = $this->connection->query('SELECT * FROM stl_load_errors WHERE query = pg_last_query_id();')->fetchAll();
            if (!count($result)) {
                throw $e;
            }

            $messages = [];
            foreach ($result as $row) {
                $messages[] = "Line $row[line_number] - $row[err_reason]";
            }
            $message = 'Load error: ' . implode("\n", $messages);

            throw new Exception($message, Exception::INVALID_SOURCE_DATA, $e);
        }
    }

    private function generateCopyCommand(string $tempTableName, array $columns, CsvFile $csvFile, array $options): string
    {
        $tableNameEscaped = $this->tableNameEscaped($tempTableName);
        $columnsSql = implode(', ', array_map(function ($column) {
            return $this->quoteIdentifier($column);
        }, $columns));

        $command = "COPY $tableNameEscaped ($columnsSql) "
            . " FROM {$this->connection->quote($csvFile->getPathname())}"
            . " CREDENTIALS 'aws_access_key_id={$this->s3key};aws_secret_access_key={$this->s3secret}' "
            . " DELIMITER '{$csvFile->getDelimiter()}' "
            . " REGION '{$this->s3region}'";

        if ($csvFile->getEnclosure()) {
            $command .= "QUOTE {$this->connection->quote($csvFile->getEnclosure())} ";
        }

        if ($csvFile->getEscapedBy()) {
            // raw format
            if ($csvFile->getEscapedBy() !== '\\') {
                throw new Exception('Only backshlash can be used as escape character');
            }
            $command .= ' ESCAPE ';
        } else {
            $command .= ' CSV ';
        }

        if (!empty($options['isGzipped'])) {
            $command .= ' GZIP ';
        }

        if (!empty($options['isManifest'])) {
            $command .= ' MANIFEST ';
        }

        $command .= ' IGNOREHEADER ' . $this->getIgnoreLines();

        // custom options
        if (!empty($options['copyOptions'])) {
            $command .= ' ' . implode(' ', $options['copyOptions']);
        }

        return $command;
    }

    private function isGzipped(string $path): bool
    {
        return in_array(pathinfo($path, PATHINFO_EXTENSION), ['gz', 'gzip']);
    }

    private function downloadManifest(string $path): array
    {
        $s3Client = new S3Client([
            'credentials' => [
                'key' => $this->s3key,
                'secret' => $this->s3secret,
            ],
            'region' => $this->s3region,
            'version' => '2006-03-01',
        ]);

        $path = parse_url($path);

        $response = $s3Client->getObject([
            'Bucket' => $path['host'],
            'Key' => ltrim($path['path'], '/'),
        ]);

        return json_decode((string) $response['Body'], true);
    }
}
