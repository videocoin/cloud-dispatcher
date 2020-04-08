-- +goose Up
-- SQL in this section is executed when the migration is applied.
ALTER TABLE tasks ADD `user_id` VARCHAR(255) DEFAULT NULL;

-- +goose Down
-- SQL in this section is executed when the migration is rolled back.
ALTER TABLE tasks DROP `user_id`;
