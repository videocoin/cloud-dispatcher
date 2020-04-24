-- +goose Up
-- SQL in this section is executed when the migration is applied.
ALTER TABLE tasks ADD `is_lock` TINYINT(1) NOT NULL DEFAULT 0;


-- +goose Down
-- SQL in this section is executed when the migration is rolled back.
ALTER TABLE tasks DROP `is_lock`;
