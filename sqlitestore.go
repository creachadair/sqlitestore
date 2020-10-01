// Copyright (C) 2020 Michael J. Fromberger. All Rights Reserved.

// Package sqlitestore implements the blob.Store interface using SQLite3.
package sqlitestore

import (
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"runtime"

	"crawshaw.io/sqlite"
	"crawshaw.io/sqlite/sqlitex"
	"github.com/creachadair/ffs/blob"
)

// A Store implements the blob.Store interface using a SQLite3 database.
type Store struct {
	pool      *sqlitex.Pool
	tableName string

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
		getStmt:     fmt.Sprintf(`SELECT value FROM %s WHERE key = $key;`, tableName),
		putStmtRep:  fmt.Sprintf(`REPLACE into %s (key, value) VALUES ($key, $value);`, tableName),
		putStmtNRep: fmt.Sprintf(`INSERT into %s (key, value) VALUES ($key, $value);`, tableName),
		sizeStmt:    fmt.Sprintf(`SELECT length(value) AS size FROM %s WHERE key = $key;`, tableName),
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
		return nil, blob.ErrKeyNotFound
	}
	data := make([]byte, stmt.GetLen("value"))
	stmt.GetBytes("value", data)
	return data, nil
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

	stmt.SetText("$key", encodeKey(opts.Key))
	stmt.SetZeroBlob("$value", int64(len(opts.Data)))

	// We need a savepoint to update the blob separately from inserting its row.
	defer sqlitex.Save(conn)(&err)

	if _, err := stmt.Step(); err != nil {
		e := err.(sqlite.Error)
		if e.Code == sqlite.SQLITE_CONSTRAINT_UNIQUE {
			return blob.ErrKeyExists
		}
		return fmt.Errorf("put: %w", err)
	}

	// Ideally we would just write the blob as a parameter using SetBytes, but
	// that doesn't work because the driver treats the blob as text.
	// See: https://github.com/crawshaw/sqlite/issues/94
	//
	// As a workaround, write the blob via the streaming interface.  At this
	// point we know we successfully inserted or replaced the row for the blob.
	blob, err := conn.OpenBlob("main", s.tableName, "value", conn.LastInsertRowID(), true)
	if err != nil {
		return fmt.Errorf("open blob: %w", err)
	} else if _, err := blob.Write(opts.Data); err != nil {
		return fmt.Errorf("write blob: %w", err)
	} else if err := blob.Close(); err != nil {
		return fmt.Errorf("close blob: %w", err)
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
		return 0, blob.ErrKeyNotFound
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
		return blob.ErrKeyNotFound
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
