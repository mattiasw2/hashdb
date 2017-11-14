-- :name create-latest! :! :n
-- :doc creates a new latest record
INSERT INTO latest
(id, tenant, entity, data, updated, version, parent)
VALUES (:id, :tenant, :entity, :data, :updated, :version, :parent)

-- :name update-latest! :! :n
-- :doc update an existing latest record
UPDATE latest
SET data = :data, updated = :updated, parent = :parent, version = :version
WHERE id = :id AND version = :parent AND tenant = :tenant
-- WHERE id = :id
-- tz problem WHERE id = :id and updated = :parent

-- :name get-latest :? :1
-- :doc retrieve a latest given the id.
SELECT id, tenant, entity, version, data, updated FROM latest
WHERE id = :id

-- :name select-all-latest :? :*
-- :doc retrieve all rows
SELECT id, tenant, entity, version, data, updated FROM latest
WHERE tenant = :tenant

-- :name select-all-latest-by-entity :? :*
-- :doc retrieve all rows
SELECT id, tenant, entity, version, data, updated FROM latest
WHERE entity = :entity AND tenant = :tenant


-- :name select-all-latest-null-entity :? :*
-- :doc retrieve all rows
SELECT id, tenant, entity, version, data, updated FROM latest
WHERE entity IS NULL AND tenant = :tenant


-- :name select-by-string-index :? :*
-- :doc retrieve all rows
SELECT l.id, l.tenant, l.entity, l.version, l.data, l.updated
FROM   latest as l , string_index as si
WHERE  l.entity  = :entity
AND    l.tenant  = :tenant
AND    si.entity = :entity
AND    l.id      = si.id
AND    si.k      = :k
AND    si.index_data = :index_data

-- :name select-by-string-index-global :? :*
-- :doc retrieve all rows
SELECT l.id, l.tenant, l.entity, l.version, l.data, l.updated
FROM   latest as l , string_index as si
WHERE  l.entity  = :entity
AND    si.entity = :entity
AND    l.id      = si.id
AND    si.k      = :k
AND    si.index_data = :index_data


-- :name delete-latest! :! :n
-- :doc delete a latest given the id
DELETE FROM latest
WHERE id = :id AND tenant = :tenant


-- :name create-history! :! :n
-- :doc creates a new history record
INSERT INTO history
(id, tenant, entity, `deleted`, `before`, `after`, updated, version, parent, is_merge, userid, sessionid, comment)
VALUES (:id, :tenant, :entity, :deleted, :before, :after, :updated, :version, :parent, :is_merge, :userid, :sessionid, :comment)


-- :name select-history :? :*
-- :doc retrieve all history rows for a latest entry
SELECT * FROM history
WHERE ID = :id
AND   tenant = :tenant


-- :name select-history-by-entity :? :*
-- :doc retrieve all history rows for a latest entry
SELECT * FROM history
WHERE entity = :entity
AND   tenant = :tenant


-- :name select-history-null-entity :? :*
-- :doc retrieve all history rows for a latest entry
SELECT * FROM history
WHERE entity is null
AND   tenant = :tenant


-- :name select-history-short :? :*
-- :doc retrieve all history rows for a latest entry
SELECT id, entity, `deleted`, updated, version, parent, is_merge, userid, sessionid, comment FROM history
WHERE ID = :id
AND   tenant = :tenant

-- ;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

-- :name create-string-index! :! :n
-- :doc creates a new string index record
INSERT INTO string_index
(id, entity, k, index_data)
VALUES (:id, :entity, :k, :index_data)

-- :name upsert-string-index! :! :n
-- :doc create or update an existing string index record
INSERT INTO string_index
(entity, k, id, index_data)
VALUES (:entity, :k, :id, :index_data)
ON DUPLICATE KEY
UPDATE index_data = :index_data

-- https://chartio.com/resources/tutorials/how-to-insert-if-row-does-not-exist-upsert-in-mysql/
-- https://dev.mysql.com/doc/refman/5.5/en/insert-on-duplicate.html


-- :name upsert-string-index-using-replace! :! :n
-- :doc create or update an existing string index record
-- https://chartio.com/resources/tutorials/how-to-insert-if-row-does-not-exist-upsert-in-mysql/
REPLACE INTO string_index
(id, entity, k, index_data)
VALUES (:id, :entity, :k, :index_data)



-- :name update-string-index! :! :n
-- :doc update an existing string index record
UPDATE string_index
SET index_data = :index_data
WHERE id = :id AND entity = :entity AND k = :k

-- :name delete-string-index! :! :n
-- :doc delete a string index given the id
DELETE FROM string_index
WHERE id = :id

-- :name delete-single-string-index! :! :n
-- :doc delete a string index given the id and entity
DELETE FROM string_index
WHERE id = :id AND entity = :entity AND k = :k


-- :name select-string-index :? :*
-- :doc retrieve all string index rows for id
SELECT * FROM string_index
WHERE ID = :id

-- :name find-string-index :? :*
-- :doc retrieve all entity and id that matches index_data and entity
-- SELECT entity, id FROM string_index
-- WHERE entity = :entity AND k = :k AND index_data = :index_data
