<?php

declare(strict_types=1);

namespace Keboola\Db\Import\Snowflake;

use Keboola\Csv\CsvFile;
use Keboola\Db\Import\Exception;
use Tracy\Debugger;
use Aws\Exception\AwsException;

abstract class CsvImportBase extends ImportBase
{
    private const SLICED_FILES_CHUNK_SIZE = 1000;

    /** @var string */
    protected $s3key;

    /** @var string */
    protected $s3secret;

    /** @var string */
    protected $s3region;

    public function __construct(
        Connection $connection,
        string $s3key,
        string $s3secret,
        string $s3region,
        string $schemaName
    ) {
        parent::__construct($connection, $schemaName);
        $this->s3key = $s3key;
        $this->s3secret = $s3secret;
        $this->s3region = $s3region;
    }

    /**
     * @param string $tableName
     * @param CsvFile $csvFile
     * @param array $options
     *  - isManifest
     * @throws Exception
     */
    protected function importTable(string $tableName, CsvFile $csvFile, array $options): void
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
            if (!empty($options['isManifest'])) {
                $this->importTableFromSlicedFile($tableName, $csvFile);
            } else {
                $this->importTableFromSingleFile($tableName, $csvFile);
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

    private function importTableFromSingleFile(string $stableName, CsvFile $csvFile)
    {
        $this->executeCopyCommand(
            $this->generateSingleFileCopyCommand($stableName, $csvFile)
        );
    }

    private function importTableFromSlicedFile(string $tableName, CsvFile $csvFile)
    {
        $slicesPaths = $this->getFilesToDownloadFromManifest($csvFile->getPathname());
        foreach (array_chunk($slicesPaths, self::SLICED_FILES_CHUNK_SIZE) as $slicesChunk) {
            $this->executeCopyCommand(
              $this->generateSlicedFileCopyCommand($tableName, $csvFile, $slicesChunk)
            );
        }
    }

    private function executeCopyCommand($sql)
    {
        $results = $this->connection->fetchAll($sql);
        foreach ($results as $result) {
            $this->importedRowsCount += (int) $result['rows_loaded'];
        }
    }

    private function generateSingleFileCopyCommand(string $tableName, CsvFile $csvFile): string
    {
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
            implode(
                ' ',
                $this->createCopyCommandCsvOptions(
                    $csvFile,
                    $this->getIgnoreLines()
                )
            )
        );
    }

    private function generateSlicedFileCopyCommand(string $tableName, CsvFile $csvFile, array $slicesPaths)
    {
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
            implode(
                ' ',
                $this->createCopyCommandCsvOptions(
                    $csvFile,
                    $this->getIgnoreLines()
                )
            ),
            implode(
                ', ',
                array_map(
                    function ($file) use ($s3Prefix) {
                        return $this->quote(str_replace($s3Prefix . '/', '', $file));
                    },
                    $slicesPaths
                )
            )
        );
    }

    private function createCopyCommandCsvOptions(CsvFile $csvFile, int $ignoreLinesCount)
    {
        $csvOptions = [];
        $csvOptions[] = sprintf('FIELD_DELIMITER = %s', $this->quote($csvFile->getDelimiter()));

        if ($ignoreLinesCount > 0) {
            $csvOptions[] = sprintf('SKIP_HEADER = %d', $ignoreLinesCount);
        }

        if ($csvFile->getEnclosure()) {
            $csvOptions[] = sprintf("FIELD_OPTIONALLY_ENCLOSED_BY = %s", $this->quote($csvFile->getEnclosure()));
            $csvOptions[] = "ESCAPE_UNENCLOSED_FIELD = NONE";
        } elseif ($csvFile->getEscapedBy()) {
            $csvOptions[] = sprintf("ESCAPE_UNENCLOSED_FIELD = %s", $this->quote($csvFile->getEscapedBy()));
        }

        return $csvOptions;
    }


    private function quote(string $value): string
    {
        return "'" . addslashes($value) . "'";
    }

    private function getFilesToDownloadFromManifest(string $path): array
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
