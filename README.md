# Keboola Database table importer 
[![Build Status](https://travis-ci.org/keboola/php-db-import.svg?branch=master)](https://travis-ci.org/keboola/php-db-import)

Handling of large bulk data into database tables.

### Supported engines:
- `MySQL` - load data from local CSV file
- `AWS Redshift`
  - Load data from CSV stored in AWS S3
  - Load data from another Redshift table in same database

### Features
- Full load - destination table is truncated before load
- Incremental load - data are merged
- Primary key dedup for all engines


### Development
Run tests with following command.

```
docker-compose run
  -e REDSHIFT_HOST=$REDSHIFT_HOST \
  -e REDSHIFT_USER=$REDSHIFT_USER \
  -e REDSHIFT_PORT=$REDSHIFT_PORT \
  -e REDSHIFT_DATABASE=$REDSHIFT_DATABASE \
  -e REDSHIFT_PASSWORD=$REDSHIFT_PASSWORD \
  -e AWS_ACCESS_KEY=$AWS_ACCESS_KEY \
  -e AWS_SECRET_KEY=$AWS_SECRET_KEY \
  -e AWS_S3_BUCKET=$AWS_S3_BUCKET
  -e AWS_REGION=us-east-1 \
  tests
```

Tests are executed agains real backends `MySQL` is provisioned and wired by Docker. Redshift and S3 credentials have to be provided.
