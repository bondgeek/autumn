CREATE DATABASE IF NOT EXISTS autumn_test;

use autumn_test;

CREATE TABLE IF NOT EXISTS `author` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `first_name` varchar(40) NOT NULL,
  `last_name` varchar(40) NOT NULL,
  `bio` text,
  PRIMARY KEY (`id`)
) ;


