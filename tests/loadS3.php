<?php

declare(strict_types=1);

/**
 * Loads test fixtures into S3
 */

date_default_timezone_set('Europe/Prague');
ini_set('display_errors', '1');
error_reporting(E_ALL);

$basedir = dirname(__DIR__);

require_once $basedir . '/vendor/autoload.php';

$client =  new \Aws\S3\S3Client([
    'region' => getenv('AWS_REGION'),
    'version' => '2006-03-01',
]);

// Where the files will be source from
$source = $basedir . '/tests/_data/csv-import';

// Where the files will be transferred to
$bucket = getenv('AWS_S3_BUCKET');
$dest = 's3://' . $bucket;

// clear bucket
$result = $client->listObjects([
    'Bucket' => $bucket,
    'Delimiter' => '/',
]);

$objects = $result->get('Contents');
if ($objects) {
    $client->deleteObjects([
        'Bucket' => $bucket,
        'Delete' => [
            'Objects' => array_map(function ($object) {
                return [
                    'Key' => $object['Key'],
                ];
            }, $objects),
        ],
    ]);
}

// generate files
$largeManifest = [
    'entries' => [],
];
for ($i = 0; $i <= 1500; $i++) {
    $sliceName = sprintf('sliced.csv_%d', $i);
    file_put_contents(
        $source . '/manifests/2cols-large/' . $sliceName,
        str_repeat("\"a\",\"b\"\n", 1000000)
    );
    $largeManifest['entries'][] = [
        'url' => sprintf("s3://%s/manifests/2cols-large/%s", $bucket, $sliceName),
        'mandatory' => true,
    ];
}
file_put_contents(
    $source . '/manifests/2cols-large/sliced.csvmanifest',
    json_encode($largeManifest)
);

// Create a transfer object.
$manager = new \Aws\S3\Transfer($client, $source, $dest, [
    'debug' => true,
]);


// Perform the transfer synchronously.
$manager->transfer();

// Create manifests

// 1. not compressed manifest
$manifest = [
    'entries' => [
        [
            'url' => sprintf("s3://%s/manifests/accounts/tw_accounts.csv0000_part_00", $bucket),
            'mandatory' => true,
        ],
        [
            'url' => sprintf("s3://%s/manifests/accounts/tw_accounts.csv0001_part_00", $bucket),
            'mandatory' => true,
        ],
    ],
];

$client->putObject([
    'Bucket' => $bucket,
    'Key' => 'manifests/accounts/tw_accounts.csvmanifest',
    'Body' => json_encode($manifest),
]);

// 2. compressed manifest
$manifest = [
    'entries' => [
        [
            'url' => sprintf("s3://%s/manifests/accounts-gzip/tw_accounts.csv.gz0000_part_00.gz", $bucket),
            'mandatory' => true,
        ],
        [
            'url' => sprintf("s3://%s/manifests/accounts-gzip/tw_accounts.csv.gz0001_part_00.gz", $bucket),
            'mandatory' => true,
        ],
    ],
];

$client->putObject([
    'Bucket' => $bucket,
    'Key' => 'manifests/accounts-gzip/tw_accounts.csv.gzmanifest',
    'Body' => json_encode($manifest),
]);


// 3. invalid manifest
$manifest = [
    'entries' => [
        [
            'url' => sprintf("s3://%s/not-exists.csv", $bucket),
            'mandatory' => true,
        ],
    ],
];

// 4. More than 1000 slices

$client->putObject([
    'Bucket' => $bucket,
    'Key' => '02_tw_accounts.csv.invalid.manifest',
    'Body' => json_encode($manifest),
]);


echo "Data loaded OK\n";
