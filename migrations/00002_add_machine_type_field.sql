-- +goose Up
-- SQL in this section is executed when the migration is applied.
ALTER TABLE tasks ADD `machine_type` varchar(255) DEFAULT NULL;


-- +goose Down
-- SQL in this section is executed when the migration is rolled back.
ALTER TABLE tasks DROP `machine_type`;
