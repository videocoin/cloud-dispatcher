package dbrutil

import (
	"context"

	"github.com/mailru/dbr"
)

type key int

const (
	dbSessionKey key = 1
	dbTxKey      key = 2
)

func NewContextWithDbSession(ctx context.Context, session *dbr.Session) context.Context {
	return context.WithValue(ctx, dbSessionKey, session)
}

func DbSessionFromContext(ctx context.Context) (*dbr.Session, bool) {
	session, ok := ctx.Value(dbSessionKey).(*dbr.Session)
	return session, ok
}

func NewContextWithDbTx(ctx context.Context, tx *dbr.Tx) context.Context {
	return context.WithValue(ctx, dbTxKey, tx)
}

func DbTxFromContext(ctx context.Context) (*dbr.Tx, bool) {
	tx, ok := ctx.Value(dbTxKey).(*dbr.Tx)
	return tx, ok
}
