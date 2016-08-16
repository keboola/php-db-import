<?php
/**
 * Created by PhpStorm.
 * User: martinhalamicek
 * Date: 27/04/16
 * Time: 15:59
 */
namespace Keboola\Db\Import\Snowflake;

use Aws\Exception\AwsException;
use Aws\S3\Exception\S3Exception;
use Keboola\Csv\CsvFile;
use Keboola\Db\Import\Exception;

class CsvManifestImport extends CsvImportBase
{
    protected function importDataToStagingTable($stagingTableName, $columns, $sourceData)
    {
        foreach ($sourceData as $csvFile) {
            $this->importFile($stagingTableName, $csvFile);
        }
    }

    private function importFile($stagingTableName, CsvFile $csvFile)
    {
        $files = $this->getFilesToDownloadFromManifest($csvFile->getPathname());
        foreach ($files as $path) {
            $newCsvPath = new CsvFile(
                $path,
                $csvFile->getDelimiter(),
                $csvFile->getEnclosure(),
                $csvFile->getEscapedBy()
            );
            $this->importTable($stagingTableName, $newCsvPath);
        }
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

        $manifest = json_decode((string)$response['Body'], true);

        return array_map(function ($entry) use ($s3Client) {
            $path = parse_url($entry['url']);

            try {
                // file validation
                $s3Client->headObject([
                    'Bucket' => $path['host'],
                    'Key' => ltrim($path['path'], '/'),
                ]);
            } catch (S3Exception $e) {
                throw new Exception(
                    sprintf('File "%s" download error: %s', $entry['url'], $e->getMessage()),
                    Exception::MANDATORY_FILE_NOT_FOUND
                );
            }

            return $entry['url'];
        }, $manifest['entries']);
    }
}
