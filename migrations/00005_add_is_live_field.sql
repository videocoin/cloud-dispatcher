-- +goose Up
-- SQL in this section is executed when the migration is applied.
ALTER TABLE tasks ADD `is_live` TINYINT(1) DEFAULT 1;


-- +goose Down
-- SQL in this section is executed when the migration is rolled back.
ALTER TABLE tasks DROP `is_live`;
