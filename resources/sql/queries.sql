-- :name create-latest! :! :n
-- :doc creates a new latest record
INSERT INTO latest
(id, data, updated, version, parent)
VALUES (:id, :data, :updated, :version, :parent)

-- :name update-latest! :! :n
-- :doc update an existing latest record
UPDATE latest
SET id = :id, data = :data, updated = :updated, parent = :parent, version = :version
WHERE id = :id and version = :parent
-- WHERE id = :id
-- tz problem WHERE id = :id and updated = :parent

-- :name get-latest :? :1
-- :doc retrieve a latest given the id.
SELECT id, version, data FROM latest
WHERE id = :id

-- :name select-all-latest :? :*
-- :doc retrieve all rows
SELECT id, version, data FROM latest


-- :name delete-latest! :! :n
-- :doc delete a latest given the id
DELETE FROM latest
WHERE id = :id


-- :name create-history! :! :n
-- :doc creates a new history record
INSERT INTO history
(id, `deleted`, `before`, `after`, updated, version, parent, is_merge, userid, sessionid, comment)
VALUES (:id, :deleted, :before, :after, :updated, :version, :parent, :is_merge, :userid, :sessionid, :comment)


-- :name select-history :? :*
-- :doc retrieve all history rows for a latest entry
SELECT * FROM history
WHERE ID = :id


-- :name select-history-short :? :*
-- :doc retrieve all history rows for a latest entry
SELECT id, `deleted`, updated, version, parent, is_merge, userid, sessionid, comment FROM history
WHERE ID = :id
