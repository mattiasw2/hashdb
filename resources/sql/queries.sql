-- :name create-latest! :! :n
-- :doc creates a new latest record
INSERT INTO latest
(id, data, updated, parent)
VALUES (:id, :data, :updated, :parent)

-- :name update-latest! :! :n
-- :doc update an existing latest record
UPDATE latest
SET id = :id, data = :data, updated = :updated, parent = :parent
WHERE id = :id and updated = :parent
-- WHERE id = :id
-- tz problem WHERE id = :id and updated = :parent

-- :name get-latest :? :1
-- :doc retrieve a latest given the id.
SELECT * FROM latest
WHERE id = :id

-- :name select-all-latest :? :*
-- :doc retrieve all rows
SELECT data FROM latest


-- :name delete-latest! :! :n
-- :doc delete a latest given the id
DELETE FROM latest
WHERE id = :id
