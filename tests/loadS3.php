<?php
/**
 * Loads test fixtures into S3
 */

date_default_timezone_set('Europe/Prague');
ini_set('display_errors', true);
error_reporting(E_ALL);

$basedir = dirname(__DIR__);

require_once $basedir . '/vendor/autoload.php';

$client =  new \Aws\S3\S3Client([
    'region' => getenv('AWS_REGION'),
    'version' => '2006-03-01',
    'credentials' => [
        'key' => getenv('AWS_ACCESS_KEY'),
        'secret' => getenv('AWS_SECRET_KEY'),
    ],
]);

// Where the files will be source from
$source = $basedir . '/tests/_data/csv-import';

// Where the files will be transferred to
$bucket = getenv('AWS_S3_BUCKET');
$dest = 's3://' . $bucket;


// Create a transfer object.
$manager = new \Aws\S3\Transfer($client, $source, $dest, [
    'debug' => true,
]);

// Perform the transfer synchronously.
$manager->transfer();

// Create manifests
$manifest = [
    'entries' => [
        [
            'url' => sprintf("s3://%s/tw_accounts.csv", $bucket),
            'mandatory' => true,
        ]
    ]
];

$client->putObject([
    'Bucket' => $bucket,
    'Key' => '01_tw_accounts.csv.manifest',
    'Body' => json_encode($manifest),
]);

$manifest = [
    'entries' => [
        [
            'url' => sprintf("s3://%s/04_tw_accounts.csv.gz", $bucket),
            'mandatory' => true,
        ]
    ]
];

$client->putObject([
    'Bucket' => $bucket,
    'Key' => '03_tw_accounts.csv.gzip.manifest',
    'Body' => json_encode($manifest),
]);

    
$manifest = [
    'entries' => [
        [
            'url' => sprintf("s3://%s/not-exists.csv", $bucket),
            'mandatory' => true,
        ]
    ]
];

$client->putObject([
    'Bucket' => $bucket,
    'Key' => '02_tw_accounts.csv.invalid.manifest',
    'Body' => json_encode($manifest),
]);


echo "Data loaded OK\n";