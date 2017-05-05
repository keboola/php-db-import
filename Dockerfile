FROM php:7.1
MAINTAINER Martin Halamicek <martin@keboola.com>
ENV DEBIAN_FRONTEND noninteractive

RUN apt-get update \
  && apt-get install unzip git unixODBC-dev libpq-dev -y

RUN echo "memory_limit = -1" >> /usr/local/etc/php/php.ini

RUN docker-php-ext-install pdo_pgsql pdo_mysql

# Snowflake
# https://github.com/docker-library/php/issues/103
RUN set -x \
 && docker-php-source extract \
 && cd /usr/src/php/ext/odbc \
 && phpize \
 && sed -ri 's@^ *test +"\$PHP_.*" *= *"no" *&& *PHP_.*=yes *$@#&@g' configure \
 && ./configure --with-unixODBC=shared,/usr \
 && docker-php-ext-install odbc \
 && docker-php-source delete

ADD ./snowflake-odbc.deb /tmp/snowflake-odbc.deb
ADD ./docker/snowflake/simba.snowflake.ini /usr/lib/snowflake/odbc/lib/simba.snowflake.ini
RUN apt-get install -y libnss3-tools && dpkg -i /tmp/snowflake-odbc.deb

# snowflake - charset settings
ENV LANG en_US.UTF-8

RUN cd \
  && curl -sS https://getcomposer.org/installer | php \
  && ln -s /root/composer.phar /usr/local/bin/composer
