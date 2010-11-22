CREATE DATABASE IF NOT EXISTS autumn_test;

use autumn_test;

CREATE TABLE IF NOT EXISTS `author` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `first_name` varchar(40) NOT NULL,
  `last_name` varchar(40) NOT NULL,
  `bio` text,
  PRIMARY KEY (`id`)
) ;

CREATE TABLE IF NOT EXISTS `books` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `title` varchar(255) DEFAULT NULL,
  `author_id` int(11) DEFAULT NULL,
  PRIMARY KEY (`id`),
  KEY `author_id` (`author_id`)
  ) ;

  CREATE TABLE `transaction` (
`id` int(11) NOT NULL AUTO_INCREMENT,
`book_id` int(11),
`saledate` DATE,
`saletime` TIME,
PRIMARY KEY (`id`)
);
