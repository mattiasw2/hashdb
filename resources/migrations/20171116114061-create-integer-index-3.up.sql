ALTER TABLE `int_index`
ADD PRIMARY KEY (`entity` ASC, `k` ASC, `id` ASC)
;

-- primary index created in separate file, since entity can be null
