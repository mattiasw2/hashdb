-- :name create-latest! :! :n
-- :doc creates a new latest record
INSERT INTO latest
(id, entity, data, updated, version, parent)
VALUES (:id, :entity, :data, :updated, :version, :parent)

-- :name update-latest! :! :n
-- :doc update an existing latest record
UPDATE latest
SET data = :data, updated = :updated, parent = :parent, version = :version
WHERE id = :id and version = :parent
-- WHERE id = :id
-- tz problem WHERE id = :id and updated = :parent

-- :name get-latest :? :1
-- :doc retrieve a latest given the id.
SELECT id, entity, version, data FROM latest
WHERE id = :id

-- :name select-all-latest :? :*
-- :doc retrieve all rows
SELECT id, entity, version, data FROM latest

-- :name select-all-latest-by-entity :? :*
-- :doc retrieve all rows
SELECT id, entity, version, data FROM latest
where entity = :entity


-- :name select-all-latest-null-entity :? :*
-- :doc retrieve all rows
SELECT id, entity, version, data FROM latest
where entity is null


-- :name delete-latest! :! :n
-- :doc delete a latest given the id
DELETE FROM latest
WHERE id = :id


-- :name create-history! :! :n
-- :doc creates a new history record
INSERT INTO history
(id, entity, `deleted`, `before`, `after`, updated, version, parent, is_merge, userid, sessionid, comment)
VALUES (:id, :entity, :deleted, :before, :after, :updated, :version, :parent, :is_merge, :userid, :sessionid, :comment)


-- :name select-history :? :*
-- :doc retrieve all history rows for a latest entry
SELECT * FROM history
WHERE ID = :id


-- :name select-history-by-entity :? :*
-- :doc retrieve all history rows for a latest entry
SELECT * FROM history
WHERE entity = :entity


-- :name select-history-null-entity :? :*
-- :doc retrieve all history rows for a latest entry
SELECT * FROM history
WHERE entity is null


-- :name select-history-short :? :*
-- :doc retrieve all history rows for a latest entry
SELECT id, entity, `deleted`, updated, version, parent, is_merge, userid, sessionid, comment FROM history
WHERE ID = :id

-- ;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

-- :name create-string-index! :! :n
-- :doc creates a new string index record
INSERT INTO string_index
(id, entity, index_data)
VALUES (:id, :entity, :index_data)

-- :name upsert-string-index! :! :n
-- :doc create or update an existing string index record
INSERT INTO string_index
(entity, id, index_data)
VALUES (:entity, :id, :index_data)
ON DUPLICATE KEY
UPDATE index_data = :index_data

-- https://chartio.com/resources/tutorials/how-to-insert-if-row-does-not-exist-upsert-in-mysql/
-- https://dev.mysql.com/doc/refman/5.5/en/insert-on-duplicate.html

-- :name upsert-string-index-not-working! :! :n
-- :doc create or update an existing string index record
-- https://chartio.com/resources/tutorials/how-to-insert-if-row-does-not-exist-upsert-in-mysql/
INSERT INTO string_index
(id, entity, index_data)
VALUES (:id, :entity, :index_data)
ON DUPLICATE KEY UPDATE string_index
SET index_data = :index_data

-- :name upsert-string-index-not-working-2! :! :n
-- :doc create or update an existing string index record
-- https://chartio.com/resources/tutorials/how-to-insert-if-row-does-not-exist-upsert-in-mysql/
REPLACE INTO string_index
(id, entity, index_data)
VALUES (:id, :entity, :index_data)



-- :name update-string-index! :! :n
-- :doc update an existing string index record
UPDATE string_index
SET index_data = :index_data
WHERE id = :id and entity = :entity

-- :name delete-string-index! :! :n
-- :doc delete a string index given the id
DELETE FROM string_index
WHERE id = :id

-- :name delete-single-string-index! :! :n
-- :doc delete a string index given the id and entity
DELETE FROM string_index
WHERE id = :id and entity = :entity
