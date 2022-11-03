FROM php:7.1

ENV DEBIAN_FRONTEND noninteractive
ENV COMPOSER_ALLOW_SUPERUSER 1
ENV COMPOSER_PROCESS_TIMEOUT 3600
ENV LANG en_US.UTF-8

RUN apt-get update \
  && apt-get install -y unzip \
      git \
      libpq-dev \
  && rm -r /var/lib/apt/lists/*

RUN echo "memory_limit = -1" >> /usr/local/etc/php/php.ini

RUN docker-php-ext-install pdo_pgsql pdo_mysql

RUN cd \
  && curl -sS https://getcomposer.org/installer | php \
  && ln -s /root/composer.phar /usr/local/bin/composer
