-- https://dev.mysql.com/doc/refman/5.6/en/innodb-compression-usage.html
CREATE TABLE `hashdb_dev`.`latest` (
  `id` VARCHAR(36) NOT NULL,
  `tenant` VARCHAR(36) NOT NULL,
  `entity` VARCHAR(36) NOT NULL,
  `data` TEXT(30000) NOT NULL,
  `updated` TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
  `version` INT NOT NULL,
  `parent` INT NOT NULL,
  PRIMARY KEY (`id`))
 ROW_FORMAT=COMPRESSED
 KEY_BLOCK_SIZE=8;
