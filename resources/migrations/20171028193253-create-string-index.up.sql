CREATE TABLE `string_index` (
  `id` VARCHAR(36) NOT NULL,
  `entity` VARCHAR(36) NULL DEFAULT NULL,
  `index_data` VARCHAR(100) NOT NULL)
;

-- primary index created in separate file, since entity can be null
