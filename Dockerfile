#VERSION 1.0.0
FROM keboola/docker-php56-all-db
MAINTAINER Martin Halamicek <martin@keboola.com>

RUN yum -y --enablerepo=epel,remi,remi-php56 install php-pgsql

ADD . /code
WORKDIR /code
RUN echo "memory_limit = -1" >> /etc/php.ini
RUN composer install --no-interaction