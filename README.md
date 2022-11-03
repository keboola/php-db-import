# Keboola Database table importer 
[![Build Status](https://travis-ci.org/keboola/php-db-import.svg?branch=master)](https://travis-ci.org/keboola/php-db-import)

Handling of large bulk data into database tables.

### Supported engines:
- `AWS Redshift`
  - Load data from CSV stored in AWS S3
  - Load data from another Redshift table in same database

### Features
- Full load - destination table is truncated before load
- Incremental load - data are merged
- Primary key dedup for all engines
- Convert empty values to `NULL` (using `convertEmptyValuesToNull` option)

### Development

#### Preparation

- Create AWS S3 bucket and IAM user using `aws-services.json` cloudformation template.
- Create Redshift cluster
- Create `.env` file. Use output of `aws-services` cloudfront stack to fill the variables and your Redshift credentials.
```
REDSHIFT_HOST=
REDSHIFT_PORT=5439
REDSHIFT_USER=
REDSHIFT_DATABASE=
REDSHIFT_PASSWORD=

AWS_ACCESS_KEY_ID=
AWS_SECRET_ACCESS_KEY=
AWS_S3_BUCKET=
AWS_REGION=
```

Upload test fixtures to S3:
```
docker-compose run php php ./tests/loadS3.php
```

### Redshift settings
User and database are required for tests. You can create them:
```
CREATE USE keboola_db_import PASSWORD 'YOUR_PASSWORD';
CREATE DATABASE keboola_db_import;
GRANT ALL ON DATABASE keboola_db_import TO keboola_db_import;
```

#### Tests Execution
Run tests with following command.

```
docker-compose run --rm tests
```

Redshift and S3 credentials have to be provided.
