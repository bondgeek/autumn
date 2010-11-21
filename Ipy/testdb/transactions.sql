use autumn_test;

DROP TABLE IF EXISTS `transaction`;

CREATE TABLE `transaction` (
`id` int(11) NOT NULL AUTO_INCREMENT,
`book_id` int(11),
`saledate` DATE,
`saletime` TIME,
PRIMARY KEY (`id`)
);
