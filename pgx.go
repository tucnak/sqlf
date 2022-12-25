package sqlf

import (
	"context"

	"github.com/georgysavva/scany/v2/pgxscan"
	"github.com/jackc/pgconn"
	"github.com/jackc/pgx/v5"
)

type pgtag = pgconn.CommandTag
type ctx context.Context
type pgx_ interface {
	Exec(ctx ctx, sql string, args ...any) (pgtag, error)
	Query(ctx ctx, sql string, args ...any) (pgx.Rows, error)
	QueryRow(ctx ctx, sql string, args ...any) pgx.Row
}

type StmtPgx struct {
	*Stmt
	ctx  context.Context
	tx   pgx_
	args []any
}

func (s *Stmt) Via(ctx context.Context, tx pgx_) *StmtPgx {
	return &StmtPgx{s, ctx, tx, nil}
}

func (s *StmtPgx) Execf(args ...any) (pgtag, error) {
	return s.tx.Exec(s.ctx, s.Stmt.String(), args...)
}

func (s *StmtPgx) Scan(args ...any) *StmtPgx {
	return &StmtPgx{s.Stmt, s.ctx, s.tx, args}
}
func (s *StmtPgx) Row(dst ...any) error {
	row := s.tx.QueryRow(s.ctx, s.Stmt.String(), s.args...)
	return row.Scan(dst...)
}
func (s *StmtPgx) Rows(dst any) error {
	rows, err := s.tx.Query(s.ctx, s.Stmt.String(), s.args...)
	if err != nil {
		return err
	}
	return pgxscan.ScanAll(dst, rows)
}
