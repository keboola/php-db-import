ARG PHP_VERSION=8.2

FROM php:${PHP_VERSION:-8.2}-cli-trixie

ENV DEBIAN_FRONTEND noninteractive
ENV COMPOSER_ALLOW_SUPERUSER 1
ENV COMPOSER_PROCESS_TIMEOUT 3600
# snowflake - charset settings
ENV LANG en_US.UTF-8

ARG SNOWFLAKE_ODBC_VERSION=3.10.0
ARG SNOWFLAKE_GPG_KEY_ID=2A3149C82551A34A

RUN apt-get update \
  && apt-get install -y unzip \
      git \
      unixodbc \
      unixodbc-dev \
      libpq-dev \
      gpg \
      debsig-verify \
      dirmngr \
      gpg-agent \
  && rm -r /var/lib/apt/lists/*

RUN echo "memory_limit = -1" >> /usr/local/etc/php/php.ini

RUN docker-php-ext-install pdo_pgsql pdo_mysql

# Snowflake ODBC
# https://github.com/docker-library/php/issues/103#issuecomment-353674490
RUN set -ex; \
    docker-php-source extract; \
    { \
        echo '# https://github.com/docker-library/php/issues/103#issuecomment-353674490'; \
        echo 'AC_DEFUN([PHP_ALWAYS_SHARED],[])dnl'; \
        echo; \
        cat /usr/src/php/ext/odbc/config.m4; \
    } > temp.m4; \
    mv temp.m4 /usr/src/php/ext/odbc/config.m4; \
    docker-php-ext-configure odbc --with-unixODBC=shared,/usr; \
    docker-php-ext-install odbc; \
    docker-php-source delete

# SNOWFLAKE
ADD ./docker/snowflake/generic.pol /etc/debsig/policies/$SNOWFLAKE_GPG_KEY_ID/generic.pol
ADD ./docker/snowflake/simba.snowflake.ini /usr/lib/snowflake/odbc/lib/simba.snowflake.ini

# snowflake - charset settings
ENV LANG=en_US.UTF-8

RUN mkdir -p ~/.gnupg \
    && chmod 700 ~/.gnupg \
    && echo "disable-ipv6" >> ~/.gnupg/dirmngr.conf \
    && mkdir -p /etc/gnupg \
    && echo "allow-weak-digest-algos" >> /etc/gnupg/gpg.conf \
    && mkdir -p /usr/share/debsig/keyrings/$SNOWFLAKE_GPG_KEY_ID \
    && gpg --keyserver hkp://keyserver.ubuntu.com --recv-keys $SNOWFLAKE_GPG_KEY_ID \
    && gpg --export $SNOWFLAKE_GPG_KEY_ID > /usr/share/debsig/keyrings/$SNOWFLAKE_GPG_KEY_ID/debsig.gpg \
    && curl https://sfc-repo.snowflakecomputing.com/odbc/linux/$SNOWFLAKE_ODBC_VERSION/snowflake-odbc-$SNOWFLAKE_ODBC_VERSION.x86_64.deb --output /tmp/snowflake-odbc.deb \
    && debsig-verify /tmp/snowflake-odbc.deb \
    && gpg --batch --delete-key --yes $SNOWFLAKE_GPG_KEY_ID \
    && dpkg -i /tmp/snowflake-odbc.deb \
    && rm /tmp/snowflake-odbc.deb

RUN cat <<EOF > /etc/odbcinst.ini
[ODBC Drivers]
SnowflakeDSIIDriver=Installed

[SnowflakeDSIIDriver]
APILevel=1
ConnectFunctions=YYY
Description=Snowflake DSII
Driver=/usr/lib/snowflake/odbc/lib/libSnowflake.so
DriverODBCVer=$SNOWFLAKE_ODBC_VERSION
SQLLevel=1
EOF

RUN cd \
  && curl -sS https://getcomposer.org/installer | php \
  && ln -s /root/composer.phar /usr/local/bin/composer
