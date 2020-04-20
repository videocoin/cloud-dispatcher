package datastore

import (
	"time"

	"github.com/mailru/dbr"
	emitterv1 "github.com/videocoin/cloud-api/emitter/v1"
)

type TaskTx struct {
	ID                    string     `db:"id"`
	TaskID                string     `db:"task_id"`
	CreatedAt             *time.Time `db:"created_at"`
	StreamContractID      string     `db:"stream_contract_id"`
	StreamContractAddress string     `db:"stream_contract_address"`
	ChunkID               int64      `db:"chunk_id"`

	AddInputChunkTx       dbr.NullString `db:"add_input_chunk_tx"`
	AddInputChunkTxStatus dbr.NullString `db:"add_input_chunk_tx_status"`

	SubmitProofTx       dbr.NullString `db:"submit_proof_tx"`
	SubmitProofTxStatus dbr.NullString `db:"submit_proof_tx_status"`

	ValidateProofTx       dbr.NullString `db:"validate_proof_tx"`
	ValidateProofTxStatus dbr.NullString `db:"validate_proof_tx_status"`

	ScrapProofTx       dbr.NullString `db:"scrap_proof_tx"`
	ScrapProofTxStatus dbr.NullString `db:"scrap_proof_tx_status"`
}

type UpdateProof struct {
	StreamContractAddress string
	ChunkID               int64
	ProfileID             int64
	SubmitProofTx         string
	SubmitProofTxStatus   emitterv1.ReceiptStatus
	ValidateProofTx       string
	ValidateProofTxStatus emitterv1.ReceiptStatus
	ScrapProofTx          string
	ScrapProofTxStatus    emitterv1.ReceiptStatus
}

type AddInputChunk struct {
	StreamID              string
	StreamContractID      string
	ChunkID               int64
	AddInputChunkTx       string
	AddInputChunkTxStatus emitterv1.ReceiptStatus
}
