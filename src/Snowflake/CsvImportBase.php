<?php

namespace Keboola\Db\Import\Snowflake;

use Keboola\Csv\CsvFile;
use Keboola\Db\Import\Exception;
use Tracy\Debugger;
use Aws\Exception\AwsException;

abstract class CsvImportBase extends ImportBase
{
    /** @var string */
    protected $s3key;

    /** @var string */
    protected $s3secret;

    /** @var string */
    protected $s3region;

    public function __construct($connection, $s3key, $s3secret, $s3region, $schemaName)
    {
        parent::__construct($connection, $schemaName);
        $this->s3key = $s3key;
        $this->s3secret = $s3secret;
        $this->s3region = $s3region;
    }

    /**
     * @param $tableName
     * @param CsvFile $csvFile
     * @param array $options
     *  - isManifest
     * @throws Exception
     */
    protected function importTable($tableName, CsvFile $csvFile, array $options)
    {
        if ($csvFile->getEnclosure() && $csvFile->getEscapedBy()) {
            throw new Exception(
                'Invalid CSV params. Either enclosure or escapedBy must be specified for Snowflake backend but not both.',
                Exception::INVALID_CSV_PARAMS,
                null
            );
        }

        try {
            $timerName = 'copyToStaging-' . $csvFile->getBasename();
            Debugger::timer($timerName);
            $results = $this->connection->fetchAll($this->generateCopyCommand($tableName, $csvFile, $options));
            foreach ($results as $result) {
                $this->importedRowsCount += (int) $result['rows_loaded'];
            }
            $this->addTimer($timerName, Debugger::timer($timerName));
        } catch (\Throwable $e) {
            $stringCode = Exception::INVALID_SOURCE_DATA;
            if (strpos($e->getMessage(), 'was not found') !== false) {
                $stringCode = Exception::MANDATORY_FILE_NOT_FOUND;
            }
            throw new Exception('Load error: ' . $e->getMessage(), $stringCode, $e);
        }
    }

    /**
     * @param $tableName
     * @param CsvFile $csvFile
     * @param array $options
     *  - isManifest
     * @return string
     */
    private function generateCopyCommand($tableName, CsvFile $csvFile, array $options)
    {
        $csvOptions = [];
        $csvOptions[] = sprintf('FIELD_DELIMITER = %s', $this->quote($csvFile->getDelimiter()));

        if ($this->getIgnoreLines()) {
            $csvOptions[] = sprintf('SKIP_HEADER = %d', $this->getIgnoreLines());
        }

        if ($csvFile->getEnclosure()) {
            $csvOptions[] = sprintf("FIELD_OPTIONALLY_ENCLOSED_BY = %s", $this->quote($csvFile->getEnclosure()));
            $csvOptions[] = "ESCAPE_UNENCLOSED_FIELD = NONE";
        } elseif ($csvFile->getEscapedBy()) {
            $csvOptions[] = sprintf("ESCAPE_UNENCLOSED_FIELD = %s", $this->quote($csvFile->getEscapedBy()));
        }

        if (empty($options['isManifest'])) {
            return sprintf(
                "COPY INTO %s FROM %s 
                CREDENTIALS = (AWS_KEY_ID = %s AWS_SECRET_KEY = %s)
                REGION = %s
                FILE_FORMAT = (TYPE=CSV %s)",
                $this->nameWithSchemaEscaped($tableName),
                $this->quote($csvFile->getPathname()),
                $this->quote($this->s3key),
                $this->quote($this->s3secret),
                $this->quote($this->s3region),
                implode(' ', $csvOptions)
            );
        } else {
            $parsedS3Path = parse_url($csvFile->getPathname());
            $s3Prefix = 's3://' . $parsedS3Path['host'];
            return sprintf(
                "COPY INTO %s FROM %s 
                CREDENTIALS = (AWS_KEY_ID = %s AWS_SECRET_KEY = %s)
                REGION = %s
                FILE_FORMAT = (TYPE=CSV %s)
                FILES = (%s)",
                $this->nameWithSchemaEscaped($tableName),
                $this->quote($s3Prefix), // s3 bucket
                $this->quote($this->s3key),
                $this->quote($this->s3secret),
                $this->quote($this->s3region),
                implode(' ', $csvOptions),
                implode(
                    ', ',
                    array_map(
                        function ($file) use ($s3Prefix) {
                            return $this->quote(str_replace($s3Prefix . '/', '', $file));
                        },
                        $this->getFilesToDownloadFromManifest($csvFile->getPathname())
                    )
                )
            );
        }
    }

    private function quote($value)
    {
        return "'" . addslashes($value) . "'";
    }

    private function getFilesToDownloadFromManifest($path)
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

        try {
            $response = $s3Client->getObject([
                'Bucket' => $path['host'],
                'Key' => ltrim($path['path'], '/'),
            ]);
        } catch (AwsException $e) {
            throw new Exception('Unable to download file from S3: ' . $e->getMessage());
        }

        $manifest = json_decode((string) $response['Body'], true);

        return array_map(function ($entry) {
            return $entry['url'];
        }, $manifest['entries']);
    }
}
