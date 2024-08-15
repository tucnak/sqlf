package sqlf

import (
	"context"
	"errors"
	"reflect"

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
	ctx   context.Context
	tx    Conn
	s     *Stmt
	close bool
}

func (s *Stmt) Via(ctx context.Context, tx Conn) Pgx {
	return Pgx{ctx, tx, s, false}
}

func (s *Stmt) ViaClose(ctx context.Context, tx Conn) Pgx {
	p := Pgx{ctx, tx, s, true}
	return p
}

func (p Pgx) Exec() (tag pgconn.CommandTag, err error) {
	s, args := p.s.String(), p.s.Args()
	tag, err = p.tx.Exec(p.ctx, s, args...)
	if p.close {
		p.s.Close()
	}
	return
}

func (p Pgx) Row(dst ...any) (err error) {
	s, args := p.s.String(), p.s.Args()
	row := p.tx.QueryRow(p.ctx, s, args...)
	err = row.Scan(dst...)
	if p.close {
		p.s.Close()
	}
	return
}
func (p Pgx) Rows(dst any) (ret error) {
	s, args := p.s.String(), p.s.Args()
	val := reflect.ValueOf(dst)
	if val.Kind() != reflect.Ptr || val.IsNil() {
		return errors.New("dst must be a non-nil pointer")
	}
	rows, err := p.tx.Query(p.ctx, s, args...)
	if err != nil {
		ret = err
		goto close
	}
	if val.Elem().Kind() == reflect.Slice {
		ret = pgxscan.ScanAll(dst, rows)
	} else {
		ret = pgxscan.ScanOne(dst, rows)
	}
close:
	if p.close {
		p.s.Close()
	}
	return
}
