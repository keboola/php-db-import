<?php

namespace Keboola\Db\Import;

use Keboola\Csv\CsvFile;
use Tracy\Debugger;

abstract class RedshiftBaseCsv extends RedshiftBase
{
    private $s3key;
    private $s3secret;
    private $s3region;

    public function __construct(\PDO $connection, $s3key, $s3secret, $s3region, $schemaName, $legacyFullImport = false)
    {
        parent::__construct($connection, $schemaName, $legacyFullImport);
        $this->s3key = $s3key;
        $this->s3secret = $s3secret;
        $this->s3region = $s3region;
    }

    /**
     * @param $tempTableName
     * @param $columns
     * @param CsvFile $csvFile
     * @param array $options
     *  - isManifest
     *  - copyOptions
     * @throws Exception
     * @throws \Exception
     */
    protected function importTable($tempTableName, $columns, CsvFile $csvFile, array $options)
    {
        if ($csvFile->getEnclosure() && $csvFile->getEscapedBy()) {
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
        } catch (\Exception $e) {
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

    private function generateCopyCommand($tempTableName, $columns, CsvFile $csvFile, array $options)
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
            if ($csvFile->getEscapedBy() != '\\') {
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

    private function isGzipped($path)
    {
        return in_array(pathinfo($path, PATHINFO_EXTENSION), ['gz', 'gzip']);
    }

    private function downloadManifest($path)
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
