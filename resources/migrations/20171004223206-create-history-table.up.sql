CREATE TABLE `history`
(`id` VARCHAR(36) NOT NULL,
-- before {} after first insert
 `before` VARCHAR(10000) NOT NULL,
 `after` VARCHAR(10000) NOT NULL,
-- without DEFAULT CURRENT_TIMESTAMP will mysql update it on every update
--  DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
 `updated` TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
 `version` INT NOT NULL,
 `parent` INT NOT NULL,
 `is_merge` BOOLEAN NOT NULL,
 `userid` VARCHAR(36) NULL,
 `sessionid` VARCHAR(36) NULL,
 `comment` VARCHAR(1000) NULL
--  PRIMARY KEY (`id`,`updated`)
  );
