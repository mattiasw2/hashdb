ALTER TABLE `string_index`
ADD UNIQUE INDEX `prim` (`entity` ASC, `id` ASC)
;

-- primary index created in separate file, since entity can be null
