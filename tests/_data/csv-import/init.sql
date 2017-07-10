DROP TABLE IF EXISTS `csv_accounts`;
CREATE TABLE `csv_accounts` (
  `id` varchar(50) NOT NULL DEFAULT '',
  `idTwitter` varchar(50) NOT NULL DEFAULT '',
  `name` varchar(50) NOT NULL DEFAULT '',
  `import` varchar(50) NOT NULL DEFAULT '',
  `isImported` varchar(50) NOT NULL DEFAULT '',
  `apiLimitExceededDatetime` varchar(50) NOT NULL DEFAULT '',
  `analyzeSentiment` varchar(50) NOT NULL DEFAULT '',
  `importKloutScore` varchar(50) NOT NULL DEFAULT '',
  `timestamp` varchar(50) NOT NULL DEFAULT '',
  `oauthToken` varchar(50) NOT NULL DEFAULT '',
  `oauthSecret` varchar(50) NOT NULL DEFAULT '',
  `idApp` varchar(50) NOT NULL DEFAULT '',
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

DROP TABLE IF EXISTS `very-long-row`;
CREATE TABLE `very-long-row` (
  `col1` text NOT NULL,
  `col2` text NOT NULL,
  `col3` text NOT NULL,
  `col4` text NOT NULL,
  `col5` text NOT NULL,
  `col6` text NOT NULL,
  `col7` text NOT NULL,
  `col8` text NOT NULL,
  `col9` text NOT NULL,
  `col10` text NOT NULL,
  `col11` text NOT NULL,
  `col12` text NOT NULL,
  `col13` text NOT NULL,
  `col14` text NOT NULL,
  `col15` text NOT NULL,
  `col16` text NOT NULL,
  `col17` text NOT NULL,
  `col18` text NOT NULL,
  `col19` text NOT NULL,
  `col20` text NOT NULL,
  `col21` text NOT NULL,
  `col22` text NOT NULL,
  `col23` text NOT NULL,
  `col24` text NOT NULL,
  `col25` text NOT NULL,
  `col26` text NOT NULL,
  `col27` text NOT NULL,
  `col28` text NOT NULL,
  `col29` text NOT NULL,
  `col30` text NOT NULL,
  `col31` text NOT NULL,
  `col32` text NOT NULL,
  `col33` text NOT NULL,
  `col34` text NOT NULL,
  `col35` text NOT NULL,
  `col36` text NOT NULL,
  `col37` text NOT NULL,
  `col38` text NOT NULL,
  `col39` text NOT NULL,
  `col40` text NOT NULL,
  `col41` text NOT NULL,
  `col42` text NOT NULL,
  `col43` text NOT NULL,
  `col44` text NOT NULL,
  `col45` text NOT NULL,
  `col46` text NOT NULL,
  `col47` text NOT NULL,
  `col48` text NOT NULL,
  `col49` text NOT NULL,
  `col50` text NOT NULL,
  `col51` text NOT NULL,
  `col52` text NOT NULL,
  `col53` text NOT NULL,
  `col54` text NOT NULL,
  `col55` text NOT NULL,
  `col56` text NOT NULL,
  `col57` text NOT NULL,
  `col58` text NOT NULL,
  `col59` text NOT NULL,
  `col60` text NOT NULL,
  `col61` text NOT NULL,
  `col62` text NOT NULL,
  `col63` text NOT NULL,
  `col64` text NOT NULL,
  `col65` text NOT NULL,
  `col66` text NOT NULL,
  `col67` text NOT NULL,
  `col68` text NOT NULL,
  `col69` text NOT NULL,
  `col70` text NOT NULL,
  `col71` text NOT NULL,
  `col72` text NOT NULL,
  `col73` text NOT NULL,
  `col74` text NOT NULL,
  `col75` text NOT NULL,
  `col76` text NOT NULL,
  `col77` text NOT NULL,
  `col78` text NOT NULL,
  `col79` text NOT NULL,
  `col80` text NOT NULL,
  `col81` text NOT NULL,
  `col82` text NOT NULL,
  `col83` text NOT NULL,
  `col84` text NOT NULL,
  `col85` text NOT NULL,
  `col86` text NOT NULL,
  `col87` text NOT NULL,
  `col88` text NOT NULL,
  `col89` text NOT NULL,
  `col90` text NOT NULL,
  `col91` text NOT NULL,
  `col92` text NOT NULL,
  `col93` text NOT NULL,
  `col94` text NOT NULL,
  `col95` text NOT NULL,
  `col96` text NOT NULL,
  `col97` text NOT NULL,
  `col98` text NOT NULL,
  `col99` text NOT NULL,
  `col100` text NOT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8;
