-- Create syntax for TABLE 'rss_cache'
CREATE TABLE IF NOT EXISTS `rss_cache` (
  `id` int(11) unsigned NOT NULL AUTO_INCREMENT,
  `url` varchar(255) NOT NULL DEFAULT '',
  `date_pub` int(11) DEFAULT NULL,
  `drop` longtext NOT NULL,
  PRIMARY KEY (`id`),
  KEY `rss_cache_in_url` (`url`),
  KEY `rss_cache_in_url_date_pub` (`date_pub`,`url`)
) ENGINE=InnoDB AUTO_INCREMENT=5765 DEFAULT CHARSET=utf8;

-- Create syntax for TABLE 'rss_urls'
CREATE TABLE IF NOT EXISTS `rss_urls` (
  `id` int(11) unsigned NOT NULL AUTO_INCREMENT,
  `url` varchar(255) NOT NULL DEFAULT '',
  `last_fetch_time` int(11) DEFAULT NULL,
  `last_fetch_etag` varchar(255) DEFAULT NULL,
  `last_fetch_modified` varchar(255) DEFAULT NULL,
  PRIMARY KEY (`id`),
  UNIQUE KEY `url` (`url`)
) ENGINE=InnoDB AUTO_INCREMENT=535 DEFAULT CHARSET=utf8;