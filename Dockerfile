#VERSION 1.0.0
FROM keboola/docker-php56-all-db
MAINTAINER Martin Halamicek <martin@keboola.com>

RUN yum -y --enablerepo=epel,remi,remi-php56 install php-pgsql

ADD . /code
WORKDIR /code
RUN echo "memory_limit = 256m" >> /etc/php.ini
RUN composer install --no-interaction

## install snowflake drivers
RUN gunzip snowflake_linux_x8664_odbc.tgz
RUN tar -xvf snowflake_linux_x8664_odbc.tar
RUN mv snowflake_odbc /usr/bin/snowflake_odbc

ADD ./docker/snowflake/simba.snowflake.ini /etc/simba.snowflake.ini
ADD ./docker/snowflake/odbcinst.ini /etc/odbcinst.ini
RUN mkdir -p  /usr/bin/snowflake_odbc/log

ENV SIMBAINI /etc/simba.snowflake.ini
ENV SSL_DIR /usr/bin/snowflake_odbc/SSLCertificates/nssdb
ENV LD_LIBRARY_PATH /usr/bin/snowflake_odbc/lib
ENV LANG en_US.UTF-8