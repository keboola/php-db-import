<?php
/**
 * Created by PhpStorm.
 * User: martinhalamicek
 * Date: 27/04/16
 * Time: 12:00
 */

namespace Keboola\Db\Import\Snowflake;

use Keboola\Db\Import\ImportInterface;
use Keboola\Csv\CsvFile;
use Keboola\Db\Import\Exception;
use Tracy\Debugger;

class CsvImport extends ImportBase
{
    private $s3key;
    private $s3secret;
    private $s3region;

    public function __construct($connection, $s3key, $s3secret, $s3region, $schemaName)
    {
        parent::__construct($connection, $schemaName);
        $this->s3key = $s3key;
        $this->s3secret = $s3secret;
        $this->s3region = $s3region;
    }

    protected function importDataToStagingTable($stagingTableName, $columns, $sourceData)
    {
        foreach ($sourceData as $csvFile) {
            $this->importTable($stagingTableName, $columns, $csvFile);
        }
    }

    protected function importTable($tableName, $columns, CsvFile $csvFile)
    {
        if ($csvFile->getEnclosure() && $csvFile->getEscapedBy()) {
            throw new Exception('Invalid CSV params. Either enclosure or escapedBy must be specified for Redshift backend but not both.', Exception::INVALID_CSV_PARAMS,
                null);
        }

        try {
            Debugger::timer('copyToStaging');
            $res = $this->queryFetchAll($this->generateCopyCommand($tableName, $columns, $csvFile));
            $this->addTimer('copyToStaging', Debugger::timer('copyToStaging'));
        } catch (\Exception $e) {

            var_dump($e->getMessage());die;
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

    private function generateCopyCommand($tableName, $columns, CsvFile $csvFile)
    {
        $csvOptions = [];
        $csvOptions[] = sprintf('FIELD_DELIMITER = %s', $this->quote($csvFile->getDelimiter()));

        if ($this->getIgnoreLines()) {
            $csvOptions[] = sprintf('SKIP_HEADER = %d', $this->getIgnoreLines());
        }

        if ($csvFile->getEnclosure()) {
            $csvOptions[] = sprintf("FIELD_OPTIONALLY_ENCLOSED_BY = %s", $this->quote($csvFile->getEnclosure()));
        }

        $command = sprintf("COPY INTO %s FROM %s 
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

        return $command;
    }

    private function isGzipped(CsvFile $csvFile, $isManifest)
    {
        if ($isManifest) {
            $s3Client = new \Aws\S3\S3Client([
                'credentials' => [
                    'key' => $this->s3key,
                    'secret' => $this->s3secret,
                ],
                'region' => $this->s3region,
                'version' => '2006-03-01',
            ]);

            $path = parse_url($csvFile->getPathname());

            $response = $s3Client->getObject([
                'Bucket' => $path['host'],
                'Key' => ltrim($path['path'], '/'),
            ]);
            $manifest = json_decode((string)$response['Body'], true);

            $path = reset($manifest['entries'])['url'];
        } else {
            $path = $csvFile->getPathname();
        }
        return in_array(pathinfo($path, PATHINFO_EXTENSION), ['gz', 'gzip']);
    }

    private function quote($value)
    {
        $q = "'";
        return ($q . str_replace("$q", "$q$q", $value) . $q);
    }
}