DROP TABLE IF EXISTS `csv_accounts`;
CREATE TABLE `csv_accounts` (
  `id` int(10) unsigned NOT NULL AUTO_INCREMENT,
  `idTwitter` bigint(20) unsigned NULL,
  `name` varchar(50) NULL,
  `import` tinyint(1) unsigned NOT NULL DEFAULT '1',
  `isImported` tinyint(1) unsigned NOT NULL DEFAULT '0',
  `apiLimitExceededDatetime` datetime DEFAULT NULL,
  `analyzeSentiment` tinyint(1) DEFAULT '1',
  `importKloutScore` tinyint(1) DEFAULT '0',
  `timestamp` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  `oauthToken` varchar(50)  NULL,
  `oauthSecret` varchar(50) NULL,
  `idApp` smallint(5) unsigned DEFAULT NULL,
  PRIMARY KEY (`id`),
  KEY `idApp` (`idApp`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

DROP TABLE IF EXISTS `csv_breaks`;
CREATE TABLE `csv_breaks` (
  `id` int(10) unsigned NOT NULL AUTO_INCREMENT,
  `name` varchar(50) NULL,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

DROP TABLE IF EXISTS `csv_2cols`;
CREATE TABLE `csv_2cols` (
  `col1`  text NOT NULL,
  `col2` text NOT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

DROP TABLE IF EXISTS `csv_transactional`;
CREATE TABLE `csv_transactional` (
  `storageApiTransaction` int(10) unsigned NOT NULL,
  `id` bigint(10) unsigned NOT NULL,
  `name` varchar(50) NOT NULL DEFAULT '',
  `price` decimal (16,4)  NULL DEFAULT '0',
  `isDeleted` tinyint(1)  NULL DEFAULT  '0',
  PRIMARY KEY (`storageApiTransaction`, `id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

DROP TABLE IF EXISTS `csv_transactional_no_id`;
CREATE TABLE `csv_transactional_no_id` (
  `storageApiTransaction` int(10) unsigned NOT NULL,
  `id` bigint(10) unsigned NOT NULL,
  `name` varchar(50) NOT NULL DEFAULT '',
  `price` decimal (16,4)  NULL DEFAULT '0',
  `isDeleted` tinyint(1)  NULL DEFAULT  '0'
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

DROP TABLE IF EXISTS `table-with-dash`;
CREATE TABLE `table-with-dash` (
  `column`  text NOT NULL,
  `index` text NOT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8;