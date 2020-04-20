-- +goose Up
-- SQL in this section is executed when the migration is applied.
CREATE TABLE tasks_tx (
    `id`                      VARCHAR(255) PRIMARY KEY NOT NULL,
    `created_at`              TIMESTAMP DEFAULT NOW(),
    `task_id`                 VARCHAR(255) NOT NULL,
    `stream_contract_id`      VARCHAR(255) NOT NULL,
    `stream_contract_address` VARCHAR(255) NOT NULL,
    `chunk_id`                INT NOT NULL,

    `add_input_chunk_tx`        VARCHAR(255) DEFAULT NULL,
    `add_input_chunk_tx_status` VARCHAR(255) DEFAULT NULL,

    `submit_proof_tx`      VARCHAR(255) DEFAULT NULL,
    `submit_proof_tx_status`  VARCHAR(255) DEFAULT NULL,

    `validate_proof_tx`      VARCHAR(255) DEFAULT NULL,
    `validate_proof_tx_status`  VARCHAR(255) DEFAULT NULL,

    `scrap_proof_tx`      VARCHAR(255) DEFAULT NULL,
    `scrap_proof_tx_status`  VARCHAR(255) DEFAULT NULL,

    INDEX `task_idx` (`task_id`),

    FOREIGN KEY (`task_id`) REFERENCES `tasks` (`id`) ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8;


-- +goose Down
-- SQL in this section is executed when the migration is rolled back.
DROP TABLE tasks_tx;