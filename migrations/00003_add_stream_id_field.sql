-- +goose Up
-- SQL in this section is executed when the migration is applied.
ALTER TABLE tasks ADD `stream_id` varchar(255) DEFAULT '';


-- +goose Down
-- SQL in this section is executed when the migration is rolled back.
ALTER TABLE tasks DROP `stream_id`;
