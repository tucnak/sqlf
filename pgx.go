package sqlf

import (
	"context"

	"github.com/georgysavva/scany/v2/pgxscan"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

type Conn interface {
	Exec(ctx context.Context, sql string, args ...any) (pgconn.CommandTag, error)
	Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error)
	QueryRow(ctx context.Context, sql string, args ...any) pgx.Row
}

type Pgx struct {
	ctx  context.Context
	tx   Conn
	s    string
	args []any
}

func (s *Stmt) Via(ctx context.Context, tx Conn) Pgx {
	return Pgx{ctx, tx, s.String(), s.Args()}
}

func (s *Stmt) ViaClose(ctx context.Context, tx Conn) Pgx {
	p := Pgx{ctx, tx, s.String(), s.Args()}
	s.Close()
	return p
}

func (p Pgx) Exec() (pgconn.CommandTag, error) {
	return p.tx.Exec(p.ctx, p.s, p.args...)
}

func (p Pgx) Row(dst ...any) error {
	row := p.tx.QueryRow(p.ctx, p.s, p.args...)
	return row.Scan(dst...)
}
func (p Pgx) Rows(dst any) error {
	rows, err := p.tx.Query(p.ctx, p.s, p.args...)
	if err != nil {
		return err
	}
	return pgxscan.ScanAll(dst, rows)
}
