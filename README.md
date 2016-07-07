# Keboola Database table importer 
[![Build Status](https://travis-ci.org/keboola/php-db-import.svg?branch=master)](https://travis-ci.org/keboola/php-db-import)

Handling of large bulk data into database tables.

### Supported engines:
- `MySQL` - load data from local CSV file
- `AWS Redshift`, `Snowflake`
  - Load data from CSV stored in AWS S3
  - Load data from another Redshift table in same database

### Features
- Full load - destination table is truncated before load
- Incremental load - data are merged
- Primary key dedup for all engines

### Development

#### Preparation

- Create AWS S3 bucket and IAM user using `aws-services.json` cloudformation template.
- Create Redshift cluster
- Create `set-env.sh` from `set-env.template.sh`. Use output of `aws-services` cloudfront stack to fill the variables and your Redshift credentials.
- Set environment `source ./set-env.sh`

Upload test fixtures to S3:
```
docker-compose run \
    -e AWS_ACCESS_KEY=$AWS_ACCESS_KEY \
    -e AWS_SECRET_KEY=$AWS_SECRET_KEY \
    -e AWS_S3_BUCKET=$AWS_S3_BUCKET \
    -e AWS_REGION=$AWS_REGION \
    php php ./tests/loadS3.php
```

#### Snowflake settings
Role, user, database and warehouse are required for tests. You can create them:
```
CREATE ROLE "KEBOOLA_DB_IMPORT";
CREATE DATABASE "KEBOOLA_DB_IMPORT";
GRANT ALL PRIVILEGES ON DATABASE "KEBOOLA_DB_IMPORT" TO ROLE "KEBOOLA_DB_IMPORT";

CREATE WAREHOUSE "KEBOOLA_DB_IMPORT" WITH WAREHOUSE_SIZE = 'XSMALL' WAREHOUSE_TYPE = 'STANDARD' AUTO_SUSPEND = 3600 AUTO_RESUME = TRUE;
GRANT USAGE ON WAREHOUSE "KEBOOLA_DB_IMPORT" TO ROLE "KEBOOLA_DB_IMPORT" WITH GRANT OPTION;

CREATE USER "KEBOOLA_DB_IMPORT"
PASSWORD = "YOUR_PASSWORD"
DEFAULT_ROLE = "KEBOOLA_DB_IMPORT";

GRANT ROLE "KEBOOLA_DB_IMPORT" TO USER "KEBOOLA_DB_IMPORT";
```
#### Tests Execution
Run tests with following command.

```
docker-compose run \
  -e REDSHIFT_HOST=$REDSHIFT_HOST \
  -e REDSHIFT_USER=$REDSHIFT_USER \
  -e REDSHIFT_PORT=$REDSHIFT_PORT \
  -e REDSHIFT_DATABASE=$REDSHIFT_DATABASE \
  -e REDSHIFT_PASSWORD=$REDSHIFT_PASSWORD \
  -e SNOWFLAKE_HOST=$SNOWFLAKE_HOST \
  -e SNOWFLAKE_PORT=$SNOWFLAKE_PORT \
  -e SNOWFLAKE_USER=$SNOWFLAKE_USER \
  -e SNOWFLAKE_PASSWORD=$SNOWFLAKE_PASSWORD \
  -e SNOWFLAKE_DATABASE=$SNOWFLAKE_DATABASE \
  -e SNOWFLAKE_WAREHOUSE=$SNOWFLAKE_WAREHOUSE \
  -e AWS_ACCESS_KEY=$AWS_ACCESS_KEY \
  -e AWS_SECRET_KEY=$AWS_SECRET_KEY \
  -e AWS_S3_BUCKET=$AWS_S3_BUCKET \
  -e AWS_REGION=$AWS_REGION \
  tests
```

Tests are executed against real backends `MySQL` is provisioned and wired by Docker. Redshift and S3 credentials have to be provided.
