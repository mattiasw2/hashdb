CREATE TABLE `history`
(`id` VARCHAR(36) NOT NULL,
 `entity` VARCHAR(36) NULL DEFAULT NULL,
 `deleted` BOOLEAN NOT NULL,
-- before {} after first insert
 `before` TEXT(30000) NOT NULL,
 `after` TEXT(30000) NOT NULL,
-- without DEFAULT CURRENT_TIMESTAMP will mysql update it on every update
--  DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
 `updated` TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
 `version` INT NOT NULL,
 `parent` INT NOT NULL,
-- for the non-acid update, (inc parent) will not do, maybe I need to using timestamp again?
 `is_merge` BOOLEAN NOT NULL,
 `userid` VARCHAR(36) NULL,
 `sessionid` VARCHAR(36) NULL,
 `comment` VARCHAR(1000) NULL
--  PRIMARY KEY (`id`,`updated`)
  );
