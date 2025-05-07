<?php

declare(strict_types=1);

namespace Keboola\Db\Import\Snowflake;

use Aws\Exception\AwsException;
use Aws\S3\S3Client;
use Keboola\Csv\CsvFile;
use Keboola\Db\Import\Exception;
use Throwable;
use Tracy\Debugger;

abstract class CsvImportBase extends ImportBase
{
    private const SLICED_FILES_CHUNK_SIZE = 1000;

    protected string $s3key;

    protected string $s3secret;

    protected string $s3region;

    public function __construct(
        Connection $connection,
        string $s3key,
        string $s3secret,
        string $s3region,
        string $schemaName,
        bool $skipColumnsCheck = false,
    ) {
        parent::__construct($connection, $schemaName, $skipColumnsCheck);
        $this->s3key = $s3key;
        $this->s3secret = $s3secret;
        $this->s3region = $s3region;
    }

    protected function importTableFromCsv(string $tableName, CsvFile $csvFile, bool $isSliced): void
    {
        if ($csvFile->getEnclosure() && $csvFile->getEscapedBy()) {
            throw new Exception(
                'Invalid CSV params. Either enclosure or escapedBy must be specified for Snowflake backend but not both.',
                Exception::INVALID_CSV_PARAMS,
                null,
            );
        }

        try {
            $timerName = 'copyToStaging-' . $csvFile->getBasename();
            Debugger::timer($timerName);
            if ($isSliced) {
                $this->importTableFromSlicedFile($tableName, $csvFile);
            } else {
                $this->importTableFromSingleFile($tableName, $csvFile);
            }
            $this->addTimer($timerName, Debugger::timer($timerName));
        } catch (Throwable $e) {
            $stringCode = Exception::INVALID_SOURCE_DATA;
            if (strpos($e->getMessage(), 'was not found') !== false) {
                $stringCode = Exception::MANDATORY_FILE_NOT_FOUND;
            }
            throw new Exception('Load error: ' . $e->getMessage(), $stringCode, $e);
        }
    }

    private function importTableFromSingleFile(string $stableName, CsvFile $csvFile): void
    {
        $csvOptions = $this->createCopyCommandCsvOptions(
            $csvFile,
            $this->getIgnoreLines(),
        );
        $this->executeCopyCommand(
            $this->generateSingleFileCopyCommand(
                $stableName,
                $csvFile->getPathname(),
                $csvOptions,
            ),
        );
    }

    private function importTableFromSlicedFile(string $tableName, CsvFile $csvFile): void
    {
        $csvOptions = $this->createCopyCommandCsvOptions(
            $csvFile,
            $this->getIgnoreLines(),
        );
        $parsedS3Path = parse_url($csvFile->getPathname());

        $slicesPaths = $this->getFilesToDownloadFromManifest(
            $parsedS3Path['host'],
            $parsedS3Path['path'],
        );
        foreach (array_chunk($slicesPaths, self::SLICED_FILES_CHUNK_SIZE) as $slicesChunk) {
            $this->executeCopyCommand(
                $this->generateSlicedFileCopyCommand(
                    $tableName,
                    $parsedS3Path['host'],
                    $slicesChunk,
                    $csvOptions,
                ),
            );
        }
    }

    private function executeCopyCommand(string $sql): void
    {
        $results = $this->connection->fetchAll($sql);
        foreach ($results as $result) {
            $this->importedRowsCount += (int) $result['rows_loaded'];
        }
    }

    private function generateSingleFileCopyCommand(string $tableName, string $s3path, array $csvOptions): string
    {
        return sprintf(
            'COPY INTO %s FROM %s 
                    CREDENTIALS = (AWS_KEY_ID = %s AWS_SECRET_KEY = %s)
                    REGION = %s
                    FILE_FORMAT = (TYPE=CSV %s)',
            $this->nameWithSchemaEscaped($tableName),
            $this->quote($s3path),
            $this->quote($this->s3key),
            $this->quote($this->s3secret),
            $this->quote($this->s3region),
            implode(
                ' ',
                $csvOptions,
            ),
        );
    }

    private function generateSlicedFileCopyCommand(string $tableName, string $s3Bucket, array $slicesPaths, array $csvOptions): string
    {
        $s3Prefix = sprintf('s3://%s', $s3Bucket);
        return sprintf(
            'COPY INTO %s FROM %s 
                CREDENTIALS = (AWS_KEY_ID = %s AWS_SECRET_KEY = %s)
                REGION = %s
                FILE_FORMAT = (TYPE=CSV %s)
                FILES = (%s)',
            $this->nameWithSchemaEscaped($tableName),
            $this->quote($s3Prefix),
            $this->quote($this->s3key),
            $this->quote($this->s3secret),
            $this->quote($this->s3region),
            implode(
                ' ',
                $csvOptions,
            ),
            implode(
                ', ',
                array_map(
                    function ($file) use ($s3Prefix) {
                        return $this->quote(str_replace($s3Prefix . '/', '', $file));
                    },
                    $slicesPaths,
                ),
            ),
        );
    }

    private function createCopyCommandCsvOptions(CsvFile $csvFile, int $ignoreLinesCount): array
    {
        $csvOptions = [];
        $csvOptions[] = sprintf('FIELD_DELIMITER = %s', $this->quote($csvFile->getDelimiter()));

        if ($ignoreLinesCount > 0) {
            $csvOptions[] = sprintf('SKIP_HEADER = %d', $ignoreLinesCount);
        }

        if ($csvFile->getEnclosure()) {
            $csvOptions[] = sprintf('FIELD_OPTIONALLY_ENCLOSED_BY = %s', $this->quote($csvFile->getEnclosure()));
            $csvOptions[] = 'ESCAPE_UNENCLOSED_FIELD = NONE';
        } elseif ($csvFile->getEscapedBy()) {
            $csvOptions[] = sprintf('ESCAPE_UNENCLOSED_FIELD = %s', $this->quote($csvFile->getEscapedBy()));
        }

        return $csvOptions;
    }


    private function quote(string $value): string
    {
        return "'" . addslashes($value) . "'";
    }

    private function getFilesToDownloadFromManifest(string $bucket, string $path): array
    {
        $s3Client = new S3Client([
            'credentials' => [
                'key' => $this->s3key,
                'secret' => $this->s3secret,
            ],
            'region' => $this->s3region,
            'version' => '2006-03-01',
        ]);

        try {
            $response = $s3Client->getObject([
                'Bucket' => $bucket,
                'Key' => ltrim($path, '/'),
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
