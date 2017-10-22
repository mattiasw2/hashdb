CREATE TABLE `hashdb_dev`.`latest` (
  `id` VARCHAR(36) NOT NULL,
  `data` TEXT(30000) NOT NULL,
  `updated` TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
  `version` INT NOT NULL,
  `parent` INT NOT NULL,
  PRIMARY KEY (`id`));
