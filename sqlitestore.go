// Copyright (C) 2020 Michael J. Fromberger. All Rights Reserved.

// Package sqlitestore implements the blob.Store interface using SQLite3.
package sqlitestore

import (
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"net/url"
	"runtime"
	"strconv"
	"strings"

	"crawshaw.io/sqlite"
	"crawshaw.io/sqlite/sqlitex"
	"github.com/creachadair/ffs/blob"
	"github.com/golang/snappy"
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
	pool      *sqlitex.Pool
	tableName string
	compress  bool

	// Pre-defined queries.
	getStmt     string // params: key
	putStmtRep  string // params: key, value; replace=true
	putStmtNRep string // params: key, value; replace=false
	sizeStmt    string // params: key
	deleteStmt  string // params: key
	listStmt    string // params: start
	lenStmt     string // no params
}

// New creates or opens a store at the specified database.
func New(uri string, opts *Options) (*Store, error) {
	pool, err := sqlitex.Open(uri, opts.openFlags(), opts.poolSize())
	if err != nil {
		return nil, err
	}

	// Create the table, if necessary.
	tableName := opts.tableName()
	conn := pool.Get(context.Background())
	defer pool.Put(conn)

	const createStmt = `CREATE TABLE IF NOT EXISTS %s (
   key BLOB UNIQUE NOT NULL,
   size INT,
   value BLOB NOT NULL
);`
	stmt, _, err := conn.PrepareTransient(fmt.Sprintf(createStmt, tableName))
	if err != nil {
		return nil, fmt.Errorf("creating table: %w", err)
	}
	defer stmt.Finalize()
	if _, err := stmt.Step(); err != nil {
		return nil, fmt.Errorf("creating table: %w", err)
	}

	return &Store{
		pool:        pool,
		tableName:   tableName,
		compress:    opts == nil || !opts.Uncompressed,
		getStmt:     fmt.Sprintf(`SELECT value FROM %s WHERE key = $key;`, tableName),
		putStmtRep:  fmt.Sprintf(`REPLACE into %s (key, size, value) VALUES ($key, $size, $value);`, tableName),
		putStmtNRep: fmt.Sprintf(`INSERT into %s (key, size, value) VALUES ($key, $size, $value);`, tableName),
		sizeStmt:    fmt.Sprintf(`SELECT size FROM %s WHERE key = $key;`, tableName),
		deleteStmt:  fmt.Sprintf(`DELETE FROM %s WHERE key = $key;`, tableName),
		listStmt:    fmt.Sprintf(`SELECT key FROM %s WHERE key >= $start ORDER BY key;`, tableName),
		lenStmt:     fmt.Sprintf(`SELECT count(*) AS len FROM %s;`, tableName),
	}, nil
}

// Options are options for constructing a Store.  A nil *Options is ready for
// use and provides default values as described.
type Options struct {
	// The number of connections to allow in the pool. If <= 0, use runtime.NumCPU.
	PoolSize int

	// Flags to use when opening the SQLite database.
	Flags sqlite.OpenFlags

	// The name of the table to use for blob data.  If unset, use "blobs".
	Table string

	// If true, store blobs without compression; by default blob data are
	// compressed with Snappy.
	Uncompressed bool
}

func (o *Options) poolSize() int {
	if o == nil || o.PoolSize <= 0 {
		return runtime.NumCPU()
	}
	return o.PoolSize
}

func (o *Options) openFlags() sqlite.OpenFlags {
	if o == nil {
		return 0
	}
	return o.Flags
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
func (s *Store) Close(context.Context) error { return s.pool.Close() }

// Get implements part of blob.Store.
func (s *Store) Get(ctx context.Context, key string) ([]byte, error) {
	conn := s.pool.Get(ctx)
	if conn == nil {
		return nil, context.Canceled
	}
	defer s.pool.Put(conn)
	conn.SetInterrupt(ctx.Done())

	stmt := conn.Prep(s.getStmt)
	defer stmt.Reset()
	stmt.SetText("$key", encodeKey(key))

	if ok, err := stmt.Step(); err != nil {
		return nil, fmt.Errorf("get: %w", err)
	} else if !ok {
		return nil, blob.KeyNotFound(key)
	}
	data := make([]byte, stmt.GetLen("value"))
	stmt.GetBytes("value", data)
	return s.decodeBlob(data)
}

// Put implements part of blob.Store.
func (s *Store) Put(ctx context.Context, opts blob.PutOptions) (err error) {
	conn := s.pool.Get(ctx)
	if conn == nil {
		return context.Canceled
	}
	defer s.pool.Put(conn)
	conn.SetInterrupt(ctx.Done())

	var stmt *sqlite.Stmt
	if opts.Replace {
		stmt = conn.Prep(s.putStmtRep)
	} else {
		stmt = conn.Prep(s.putStmtNRep)
	}
	defer stmt.Reset()

	enc := s.encodeBlob(opts.Data)
	stmt.SetText("$key", encodeKey(opts.Key))
	stmt.SetInt64("$size", int64(len(opts.Data))) // N.B. uncompressed size
	stmt.SetBytes("$value", enc)                  // N.B. encoded data

	if _, err := stmt.Step(); err != nil {
		e := err.(sqlite.Error)
		if e.Code == sqlite.SQLITE_CONSTRAINT_UNIQUE {
			return blob.KeyExists(opts.Key)
		}
		return fmt.Errorf("put: %w", err)
	}
	return nil
}

// Size implements part of blob.Store.
func (s *Store) Size(ctx context.Context, key string) (int64, error) {
	conn := s.pool.Get(ctx)
	if conn == nil {
		return 0, context.Canceled
	}
	defer s.pool.Put(conn)
	conn.SetInterrupt(ctx.Done())

	stmt := conn.Prep(s.sizeStmt)
	defer stmt.Reset()
	stmt.SetText("$key", encodeKey(key))
	if ok, err := stmt.Step(); err != nil {
		return 0, fmt.Errorf("size: %w", err)
	} else if !ok {
		return 0, blob.KeyNotFound(key)
	}
	return stmt.GetInt64("size"), nil
}

// Delete implements part of blob.Store.
func (s *Store) Delete(ctx context.Context, key string) error {
	conn := s.pool.Get(ctx)
	if conn == nil {
		return context.Canceled
	}
	defer s.pool.Put(conn)
	conn.SetInterrupt(ctx.Done())

	stmt := conn.Prep(s.deleteStmt)
	defer stmt.Reset()
	stmt.SetText("$key", encodeKey(key))
	if _, err := stmt.Step(); err != nil {
		return fmt.Errorf("delete: %w", err)
	} else if conn.Changes() == 0 {
		return blob.KeyNotFound(key)
	}
	return nil
}

// List implements part of blob.Store.
func (s *Store) List(ctx context.Context, start string, f func(string) error) error {
	conn := s.pool.Get(ctx)
	if conn == nil {
		return context.Canceled
	}
	defer s.pool.Put(conn)
	conn.SetInterrupt(ctx.Done())

	stmt := conn.Prep(s.listStmt)
	defer stmt.Reset()

	stmt.SetText("$start", encodeKey(start))
	for {
		ok, err := stmt.Step()
		if err != nil {
			return fmt.Errorf("list: %w", err)
		} else if !ok {
			break
		}

		key := make([]byte, stmt.GetLen("key"))
		stmt.GetBytes("key", key)
		skey := decodeKey(key)
		if err := f(skey); err == blob.ErrStopListing {
			break
		} else if err != nil {
			return err
		}
	}
	return nil
}

// Len implements part of blob.Store.
func (s *Store) Len(ctx context.Context) (int64, error) {
	conn := s.pool.Get(ctx)
	if conn == nil {
		return 0, context.Canceled
	}
	defer s.pool.Put(conn)
	conn.SetInterrupt(ctx.Done())

	stmt := conn.Prep(s.lenStmt)
	defer stmt.Reset()
	if ok, err := stmt.Step(); err != nil {
		return 0, fmt.Errorf("len: %w", err)
	} else if !ok {
		return 0, errors.New("len: internal error: no count reported")
	}
	return stmt.GetInt64("len"), nil
}
