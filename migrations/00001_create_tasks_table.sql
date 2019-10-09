-- +goose Up
-- SQL in this section is executed when the migration is applied.
CREATE TABLE tasks (
    `id`              VARCHAR(255) PRIMARY KEY NOT NULL,
    `created_at`      TIMESTAMP DEFAULT NOW(),
    `owner_id`        INT(11) NOT NULL,
    `status`          VARCHAR(30) NOT NULL,
    `profile_id`      CHAR(36) NOT NULL,
    `cmdline`         TEXT DEFAULT NULL,
    `input`           JSON,
    `output`          JSON,
    `machine_id`      CHAR(36) NULL,
    `stream_contract_id` BIGINT(10) NULL,
    `stream_contract_address` VARCHAR(255) NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8;


-- +goose Down
-- SQL in this section is executed when the migration is rolled back.
DROP TABLE tasks;