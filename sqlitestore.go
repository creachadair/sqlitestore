// Copyright (C) 2020 Michael J. Fromberger. All Rights Reserved.

// Package sqlitestore implements the blob.Store interface using SQLite3.
package sqlitestore

import (
	"context"
	"database/sql"
	"encoding/hex"
	"errors"
	"fmt"
	"net/url"
	"runtime"
	"strconv"
	"strings"

	"github.com/creachadair/ffs/blob"
	"github.com/golang/snappy"
	"modernc.org/sqlite"
)

// Opener constructs a sqlitestore from a SQLite URI, for use with the store
// package. To specify the table name, prefix addr with "tablename@".
//
// If poolsize=n is set, it is used to set the pool size.
// If compress=v is set, it is used to enable/disable compression (default true).
// Other query parameters are passed to SQLite.
func Opener(_ context.Context, addr string) (blob.Store, error) {
	var opts Options
	if i := strings.Index(addr, "@"); i > 0 {
		opts.Table, addr = addr[:i], addr[i+1:]
	}

	// Extract and remove query parameters specific to the store.
	if i := strings.Index(addr, "?"); i > 0 {
		base := addr[:i]
		q, err := url.ParseQuery(addr[i+1:])
		if err != nil {
			return nil, fmt.Errorf("invalid query: %w", err)
		}
		if ps := q.Get("poolsize"); ps != "" {
			opts.PoolSize, err = strconv.Atoi(ps)
			if err != nil {
				return nil, fmt.Errorf("invalid poolsize: %w", err)
			}
			delete(q, "poolsize")
		}
		if c := q.Get("compress"); c != "" {
			v, err := strconv.ParseBool(c)
			if err != nil {
				return nil, fmt.Errorf("invalid compress: %w", err)
			}
			opts.Uncompressed = !v
			delete(q, "compress")
		}
		addr = base
		if r := q.Encode(); r != "" {
			addr += "?" + r
		}
	}

	return New(addr, &opts)
}

// A Store implements the blob.Store interface using a SQLite3 database.
type Store struct {
	db        *sql.DB
	tableName string
	compress  bool

	// Pre-defined queries.
	getStmt     string // params: key
	putStmtRep  string // params: key, value; replace=true
	putStmtNRep string // params: key, value; replace=false
	deleteStmt  string // params: key
	listStmt    string // params: start
	lenStmt     string // no params
}

// New creates or opens a store at the specified database.
func New(uri string, opts *Options) (*Store, error) {
	db, err := sql.Open(opts.driverName(), uri)
	if err != nil {
		return nil, err
	}
	if size := opts.poolSize(); size > 0 {
		db.SetMaxOpenConns(size)
	}

	// Create the table, if necessary.
	tableName := opts.tableName()
	if _, err := db.Exec(fmt.Sprintf(`
PRAGMA journal_mode = WAL;
CREATE TABLE IF NOT EXISTS %s (
   key BLOB UNIQUE NOT NULL,
   value BLOB NOT NULL
);`, tableName)); err != nil {
		return nil, fmt.Errorf("creating table: %w", err)
	}

	return &Store{
		db:          db,
		tableName:   tableName,
		compress:    opts == nil || !opts.Uncompressed,
		getStmt:     fmt.Sprintf(`SELECT value FROM %s WHERE key = $key;`, tableName),
		putStmtRep:  fmt.Sprintf(`REPLACE into %s (key, value) VALUES ($key, $value);`, tableName),
		putStmtNRep: fmt.Sprintf(`INSERT into %s (key, value) VALUES ($key, $value);`, tableName),
		deleteStmt:  fmt.Sprintf(`DELETE FROM %s WHERE key = $key;`, tableName),
		listStmt:    fmt.Sprintf(`SELECT key FROM %s WHERE key >= $start ORDER BY key;`, tableName),
		lenStmt:     fmt.Sprintf(`SELECT count(*) AS len FROM %s;`, tableName),
	}, nil
}

// Options are options for constructing a Store.  A nil *Options is ready for
// use and provides default values as described.
type Options struct {
	// The name of the SQL driver to use, default "sqlite".
	Driver string

	// The number of connections to allow in the pool. If <= 0, use runtime.NumCPU.
	PoolSize int

	// The name of the table to use for blob data.  If unset, use "blobs".
	Table string

	// If true, store blobs without compression; by default blob data are
	// compressed with Snappy.
	Uncompressed bool
}

func (o *Options) driverName() string {
	if o == nil || o.Driver == "" {
		return "sqlite"
	}
	return o.Driver
}

func (o *Options) poolSize() int {
	if o == nil || o.PoolSize <= 0 {
		return runtime.NumCPU()
	}
	return o.PoolSize
}

func (o *Options) tableName() string {
	if o == nil || o.Table == "" {
		return "blobs"
	}
	return o.Table
}

func encodeKey(key string) string { return hex.EncodeToString([]byte(key)) }

func decodeKey(ekey []byte) string {
	n, err := hex.Decode(ekey, ekey)
	if err != nil {
		panic("invalid key")
	}
	return string(ekey[:n])
}

func (s *Store) encodeBlob(data []byte) []byte {
	if s.compress {
		return snappy.Encode(nil, data)
	}
	return data
}

func (s *Store) decodeBlob(data []byte) ([]byte, error) {
	if s.compress {
		return snappy.Decode(nil, data)
	}
	return data, nil
}

// Close implements blob.Closer.
func (s *Store) Close(ctx context.Context) error {
	// Attempt to clean up the WAL before closing.
	_, verr := s.db.Exec(`VACUUM; PRAGMA wal_checkpoint(TRUNCATE);`)

	// Even if that fails, however, make sure the pool gets cleaned up.
	cerr := s.db.Close()
	return errors.Join(verr, cerr)
}

// Get implements part of blob.Store.
func (s *Store) Get(ctx context.Context, key string) ([]byte, error) {
	return withTxValue(ctx, s.db, func(tx *sql.Tx) ([]byte, error) {
		row := tx.QueryRowContext(ctx, s.getStmt, sql.Named("key", encodeKey(key)))
		var data []byte
		if err := row.Scan(&data); errors.Is(err, sql.ErrNoRows) {
			return nil, blob.KeyNotFound(key)
		} else if err != nil {
			return nil, fmt.Errorf("get: %w", err)
		}
		return s.decodeBlob(data)
	})
}

// Put implements part of blob.Store.
func (s *Store) Put(ctx context.Context, opts blob.PutOptions) error {
	stmt := s.putStmtNRep
	if opts.Replace {
		stmt = s.putStmtRep
	}
	return withTxErr(ctx, s.db, func(tx *sql.Tx) error {
		_, err := tx.ExecContext(ctx, stmt,
			sql.Named("key", encodeKey(opts.Key)),
			sql.Named("value", s.encodeBlob(opts.Data)),
		)
		const sqliteConstraintUnique = 2067
		if serr, ok := err.(*sqlite.Error); ok && serr.Code() == sqliteConstraintUnique {
			return blob.KeyExists(opts.Key)
		} else if err != nil {
			return fmt.Errorf("put: %w", err)
		}
		return nil
	})
}

// Delete implements part of blob.Store.
func (s *Store) Delete(ctx context.Context, key string) error {
	return withTxErr(ctx, s.db, func(tx *sql.Tx) error {
		rsp, err := tx.ExecContext(ctx, s.deleteStmt, sql.Named("key", encodeKey(key)))
		if err != nil {
			return fmt.Errorf("delete: %w", err)
		} else if nr, _ := rsp.RowsAffected(); nr == 0 {
			return blob.KeyNotFound(key)
		}
		return nil
	})
}

// List implements part of blob.Store.
func (s *Store) List(ctx context.Context, start string, f func(string) error) error {
	return withTxErr(ctx, s.db, func(tx *sql.Tx) error {
		rows, err := tx.QueryContext(ctx, s.listStmt, sql.Named("start", encodeKey(start)))
		if err != nil {
			return fmt.Errorf("list: %w", err)
		}
		defer rows.Close()
		for rows.Next() {
			var key []byte
			if err := rows.Scan(&key); err != nil {
				return fmt.Errorf("list: %w", err)
			}
			skey := decodeKey(key)
			if err := f(skey); errors.Is(err, blob.ErrStopListing) {
				break
			} else if err != nil {
				return err
			}
		}
		return rows.Close()
	})
}

// Len implements part of blob.Store.
func (s *Store) Len(ctx context.Context) (int64, error) {
	return withTxValue(ctx, s.db, func(tx *sql.Tx) (int64, error) {
		row := tx.QueryRowContext(ctx, s.lenStmt)
		var nr int64
		if err := row.Scan(&nr); err != nil {
			return 0, err
		}
		return nr, nil
	})
}

func withTxValue[T any](ctx context.Context, db *sql.DB, f func(*sql.Tx) (T, error)) (T, error) {
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		var zero T
		return zero, err
	}
	defer tx.Rollback()
	v, err := f(tx)
	if err != nil {
		return v, err
	}
	return v, tx.Commit()
}

func withTxErr(ctx context.Context, db *sql.DB, f func(*sql.Tx) error) error {
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	if err := f(tx); err != nil {
		return err
	}
	return tx.Commit()
}
