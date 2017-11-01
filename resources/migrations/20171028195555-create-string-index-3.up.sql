ALTER TABLE `string_index`
ADD PRIMARY KEY (`entity` ASC, `id` ASC)
;

-- primary index created in separate file, since entity can be null
