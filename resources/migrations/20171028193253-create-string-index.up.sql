CREATE TABLE `string_index` (
  `id` VARCHAR(36) NOT NULL,
  `entity` VARCHAR(36) NOT NULL,
  `index_data` VARCHAR(100) NOT NULL)
;

-- primary index created in separate file, entity not allowed to be NULL, since
-- upsert needs primary index, and parts of primary index cannot be NULL (it seems).
