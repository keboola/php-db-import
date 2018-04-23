FROM php:7.1
ENV DEBIAN_FRONTEND noninteractive

RUN apt-get update \
  && apt-get install unzip git unixODBC-dev libpq-dev -y

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

ADD ./snowflake-odbc.deb /tmp/snowflake-odbc.deb
RUN dpkg -i /tmp/snowflake-odbc.deb
ADD ./docker/snowflake/simba.snowflake.ini /usr/lib/snowflake/odbc/lib/simba.snowflake.ini

# snowflake - charset settings
ENV LANG en_US.UTF-8

RUN cd \
  && curl -sS https://getcomposer.org/installer | php \
  && ln -s /root/composer.phar /usr/local/bin/composer
