-- +goose Up
-- SQL in this section is executed when the migration is applied.
CREATE TABLE tasks (
    `id`              VARCHAR(255) PRIMARY KEY NOT NULL,
    `created_at`      TIMESTAMP DEFAULT NOW(),
    `deleted_at`      TIMESTAMP NULL DEFAULT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8;


-- +goose Down
-- SQL in this section is executed when the migration is rolled back.
DROP TABLE tasks;