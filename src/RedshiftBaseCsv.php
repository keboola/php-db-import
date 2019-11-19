<?php

declare(strict_types=1);

namespace Keboola\Db\Import;

use Keboola\Csv\CsvOptions;
use SplFileInfo;
use Tracy\Debugger;

abstract class RedshiftBaseCsv extends RedshiftBase
{
    /** @var string */
    private $s3key;

    /** @var string */
    private $s3secret;

    /** @var string */
    private $s3region;

    public function __construct(
        \PDO $connection,
        string $s3key,
        string $s3secret,
        string $s3region,
        string $schemaName,
        bool $legacyFullImport = false
    ) {
        parent::__construct($connection, $schemaName, $legacyFullImport);
        $this->s3key = $s3key;
        $this->s3secret = $s3secret;
        $this->s3region = $s3region;
    }

    /**
     * @param string $tempTableName
     * @param array $columns
     * @param SplFileInfo $file
     * @param CsvOptions $csvOptions
     * @param array $options
     *  - isManifest
     *  - copyOptions
     * @throws Exception
     * @throws \Throwable
     */
    protected function importTable(string $tempTableName, array $columns, SplFileInfo $file, CsvOptions $csvOptions, array $options): void
    {
        if ($csvOptions->getEnclosure() && $csvOptions->getEscapedBy()) {
            throw new Exception(
                'Invalid CSV params. Either enclosure or escapedBy must be specified for Redshift backend but not both.',
                Exception::INVALID_CSV_PARAMS,
                null
            );
        }

        try {
            Debugger::timer('copyToStaging');
            $copyOptions = [
                'isManifest' => isset($options['isManifest']) ? $options['isManifest'] : false,
                'copyOptions' => isset($options['copyOptions']) ? $options['copyOptions'] : [],
            ];

            if (isset($options['isManifest']) && $options['isManifest']) {
                $manifest = $this->downloadManifest($file->getPathname());

                // empty manifest handling - do nothing
                if (!count($manifest['entries'])) {
                    $this->addTimer('copyToStaging', Debugger::timer('copyToStaging'));
                    return;
                }

                $copyOptions['isGzipped'] = $this->isGzipped(reset($manifest['entries'])['url']);
            } else {
                $copyOptions['isGzipped'] = $this->isGzipped($file->getPathname());
            }

            $this->query($this->generateCopyCommand($tempTableName, $columns, $file, $csvOptions, $copyOptions));
            $this->addTimer('copyToStaging', Debugger::timer('copyToStaging'));
        } catch (\Throwable $e) {
            $result = $this->connection->query("SELECT * FROM stl_load_errors WHERE query = pg_last_query_id();")->fetchAll();
            if (!count($result)) {
                throw $e;
            }

            $messages = [];
            foreach ($result as $row) {
                $messages[] = "Line $row[line_number] - $row[err_reason]";
            }
            $message = "Load error: " . implode("\n", $messages);

            throw new Exception($message, Exception::INVALID_SOURCE_DATA, $e);
        }
    }

    private function generateCopyCommand(string $tempTableName, array $columns, SplFileInfo $file, CsvOptions $csvOptions, array $options): string
    {
        $tableNameEscaped = $this->tableNameEscaped($tempTableName);
        $columnsSql = implode(', ', array_map(function ($column) {
            return $this->quoteIdentifier($column);
        }, $columns));

        $command = "COPY $tableNameEscaped ($columnsSql) "
            . " FROM {$this->connection->quote($file->getPathname())}"
            . " CREDENTIALS 'aws_access_key_id={$this->s3key};aws_secret_access_key={$this->s3secret}' "
            . " DELIMITER '{$csvOptions->getDelimiter()}' "
            . " REGION '{$this->s3region}'";

        if ($csvOptions->getEnclosure()) {
            $command .= "QUOTE {$this->connection->quote($csvOptions->getEnclosure())} ";
        }

        if ($csvOptions->getEscapedBy()) {
            // raw format
            if ($csvOptions->getEscapedBy() !== '\\') {
                throw new Exception('Only backshlash can be used as escape character');
            }
            $command .= " ESCAPE ";
        } else {
            $command .= " CSV ";
        }

        if (!empty($options['isGzipped'])) {
            $command .= " GZIP ";
        }

        if (!empty($options['isManifest'])) {
            $command .= " MANIFEST ";
        }

        $command .= " IGNOREHEADER " . $this->getIgnoreLines();

        // custom options
        if (!empty($options['copyOptions'])) {
            $command .= " " . implode(" ", $options['copyOptions']);
        }

        return $command;
    }

    private function isGzipped(string $path): bool
    {
        return in_array(pathinfo($path, PATHINFO_EXTENSION), ['gz', 'gzip']);
    }

    private function downloadManifest(string $path): array
    {
        $s3Client = new \Aws\S3\S3Client([
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
