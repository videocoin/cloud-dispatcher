-- +goose Up
-- SQL in this section is executed when the migration is applied.
CREATE TABLE tasks_history (
    `id`         VARCHAR(255) PRIMARY KEY NOT NULL,
    `miner_id`   VARCHAR(255) NOT NULL,
    `task_id`    VARCHAR(255) NOT NULL,
    `created_at` TIMESTAMP DEFAULT NOW(),
) ENGINE=InnoDB DEFAULT CHARSET=utf8;


-- +goose Down
-- SQL in this section is executed when the migration is rolled back.
DROP TABLE tasks_history;