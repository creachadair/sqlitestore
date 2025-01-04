// Copyright (C) 2020 Michael J. Fromberger. All Rights Reserved.

// Package sqlitestore implements the [blob.KV] interface using SQLite3.
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
	"sync"

	"github.com/creachadair/ffs/blob"
	"github.com/creachadair/ffs/storage/dbkey"
	"github.com/creachadair/mds/value"
	"github.com/golang/snappy"
	"modernc.org/sqlite"
)

// Opener constructs a sqlitestore from a SQLite URI, for use with the store
// package. To specify the table name, prefix addr with "tablename@".
//
// If poolsize=n is set, it is used to set the pool size.
// If compress=v is set, it is used to enable/disable compression (default true).
// Other query parameters are passed to SQLite.
func Opener(_ context.Context, addr string) (blob.StoreCloser, error) {
	var opts Options

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

type Store struct {
	*dbMonitor
}

// Close implements part of the [blob.StoreCloser] interface.
func (s Store) Close(ctx context.Context) error {
	s.txmu.Lock()
	defer s.txmu.Unlock()

	// Attempt to vacuum the database before closing.
	_, verr := s.db.Exec(`vacuum`)

	// Even if that fails, however, make sure the pool gets cleaned up.
	cerr := s.db.Close()
	return errors.Join(verr, cerr)
}

type dbMonitor struct {
	// These fields are read-only after initialization.
	tableName dbkey.Prefix
	compress  bool

	txmu sync.RWMutex // ex: write db, sh: read db
	db   *sql.DB
}

func (d *dbMonitor) KV(ctx context.Context, name string) (blob.KV, error) {
	ktab := d.tableName.Keyspace(name).String() // hex-encoded

	d.txmu.Lock()
	defer d.txmu.Unlock()
	if err := withTxErr(ctx, d.db, func(tx *sql.Tx) error {
		_, err := tx.ExecContext(ctx, fmt.Sprintf(`create table if not exists "%s" (
  key BLOB unique not null,
  value BLOB not null,
  vsize INTEGER not null
)`, ktab))
		return err
	}); err != nil {
		return nil, err
	}
	return KV{db: d, tableName: ktab}, nil
}

func (d *dbMonitor) CAS(ctx context.Context, name string) (blob.CAS, error) {
	kv, err := d.KV(ctx, name)
	if err != nil {
		return nil, err
	}
	return blob.CASFromKV(kv), nil
}

func (d *dbMonitor) Sub(ctx context.Context, name string) (blob.Store, error) {
	return Store{dbMonitor: &dbMonitor{
		tableName: d.tableName.Sub(name),
		compress:  d.compress,
		db:        d.db,
	}}, nil
}

// A KV implements the [blob.KV] interface using a SQLite3 database.
type KV struct {
	db        *dbMonitor
	tableName string
}

// New creates or opens a store at the specified database.
func New(uri string, opts *Options) (Store, error) {
	db, err := sql.Open(opts.driverName(), uri)
	if err != nil {
		return Store{}, err
	}
	if size := opts.poolSize(); size > 0 {
		db.SetMaxOpenConns(size)
	}
	return Store{dbMonitor: &dbMonitor{
		db:       db,
		compress: opts == nil || !opts.Uncompressed,
	}}, nil
}

// Options are options for constructing a [KV].  A nil *Options is ready for
// use and provides default values as described.
type Options struct {
	// The name of the SQL driver to use, default "sqlite".
	Driver string

	// The number of connections to allow in the pool. If <= 0, use runtime.NumCPU.
	PoolSize int

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

func encodeKey(key string) string { return hex.EncodeToString([]byte(key)) }

func decodeKey(ekey []byte) string {
	n, err := hex.Decode(ekey, ekey)
	if err != nil {
		panic("invalid key")
	}
	return string(ekey[:n])
}

func (s KV) encodeBlob(data []byte) []byte {
	if s.db.compress {
		return snappy.Encode(nil, data)
	}
	return data
}

func (s *KV) decodeBlob(data []byte) ([]byte, error) {
	if s.db.compress {
		return snappy.Decode(nil, data)
	}
	return data, nil
}

// Get implements part of [blob.KV].
func (s KV) Get(ctx context.Context, key string) ([]byte, error) {
	s.db.txmu.RLock()
	defer s.db.txmu.RUnlock()

	query := fmt.Sprintf(`select value from "%s" where key = $key`, s.tableName)
	return withTxValue(ctx, s.db.db, func(tx *sql.Tx) ([]byte, error) {
		row := tx.QueryRowContext(ctx, query, sql.Named("key", encodeKey(key)))
		var data []byte
		if err := row.Scan(&data); errors.Is(err, sql.ErrNoRows) {
			return nil, blob.KeyNotFound(key)
		} else if err != nil {
			return nil, fmt.Errorf("get: %w", err)
		}
		return s.decodeBlob(data)
	})
}

// Has implements part of [blob.KV].
func (s KV) Has(ctx context.Context, keys ...string) (blob.KeySet, error) {
	s.db.txmu.RLock()
	defer s.db.txmu.RUnlock()

	query := fmt.Sprintf(`select vsize from "%s" where key = $key`, s.tableName)
	return withTxValue(ctx, s.db.db, func(tx *sql.Tx) (blob.KeySet, error) {
		var out blob.KeySet
		for _, key := range keys {
			row := tx.QueryRowContext(ctx, query, sql.Named("key", encodeKey(key)))
			var size int64
			if err := row.Scan(&size); errors.Is(err, sql.ErrNoRows) {
				continue
			} else if err != nil {
				return nil, fmt.Errorf("stat: %w", err)
			}
			out.Add(key)
		}
		return out, nil
	})
}

// Put implements part of [blob.KV].
func (s KV) Put(ctx context.Context, opts blob.PutOptions) error {
	s.db.txmu.Lock()
	defer s.db.txmu.Unlock()

	op := value.Cond(opts.Replace, "replace", "insert")
	stmt := fmt.Sprintf(`%s into "%s" (key, value, vsize) values ($key, $value, $vsize)`, op, s.tableName)
	return withTxErr(ctx, s.db.db, func(tx *sql.Tx) error {
		_, err := tx.ExecContext(ctx, stmt,
			sql.Named("key", encodeKey(opts.Key)),
			sql.Named("value", s.encodeBlob(opts.Data)),
			sql.Named("vsize", len(opts.Data)),
		)
		const sqliteConstraintUnique = 2067
		var serr *sqlite.Error
		if errors.As(err, &serr) && serr.Code() == sqliteConstraintUnique {
			return blob.KeyExists(opts.Key)
		} else if err != nil {
			return fmt.Errorf("put: %w", err)
		}
		return nil
	})
}

// Delete implements part of [blob.KV].
func (s KV) Delete(ctx context.Context, key string) error {
	s.db.txmu.Lock()
	defer s.db.txmu.Unlock()

	stmt := fmt.Sprintf(`delete from "%s" where key = $key`, s.tableName)
	return withTxErr(ctx, s.db.db, func(tx *sql.Tx) error {
		rsp, err := tx.ExecContext(ctx, stmt, sql.Named("key", encodeKey(key)))
		if err != nil {
			return fmt.Errorf("delete: %w", err)
		} else if nr, _ := rsp.RowsAffected(); nr == 0 {
			return blob.KeyNotFound(key)
		}
		return nil
	})
}

// List implements part of [blob.KV].
func (s KV) List(ctx context.Context, start string, f func(string) error) error {
	s.db.txmu.RLock()
	defer s.db.txmu.RUnlock()

	query := fmt.Sprintf(`select key from "%s" where key >= $start order by key`, s.tableName)
	return withTxErr(ctx, s.db.db, func(tx *sql.Tx) error {
		rows, err := tx.QueryContext(ctx, query, sql.Named("start", encodeKey(start)))
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

// Len implements part of [blob.KV].
func (s KV) Len(ctx context.Context) (int64, error) {
	s.db.txmu.RLock()
	defer s.db.txmu.RUnlock()

	query := fmt.Sprintf(`select count(*) from "%s"`, s.tableName)
	return withTxValue(ctx, s.db.db, func(tx *sql.Tx) (int64, error) {
		row := tx.QueryRowContext(ctx, query)
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
