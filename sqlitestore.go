// Copyright (C) 2020 Michael J. Fromberger. All Rights Reserved.

// Package sqlitestore implements the [blob.KV] interface using SQLite3.
package sqlitestore

import (
	"context"
	"database/sql"
	"encoding/hex"
	"errors"
	"fmt"
	"iter"
	"net/url"
	"runtime"
	"strconv"
	"strings"
	"sync"

	"github.com/creachadair/ffs/blob"
	"github.com/creachadair/ffs/storage/dbkey"
	"github.com/creachadair/ffs/storage/monitor"
	"github.com/creachadair/mds/value"
	"github.com/golang/snappy"
	"modernc.org/sqlite"
)

// Opener constructs a sqlitestore from a SQLite URI, for use with the store
// package.
//
//   - If poolsize=n is set, it is used to set the pool size to n connections.
//   - If compress=v is set, it is used to enable/disable compression (default true).
//   - If journal=m is set, it is used to set the journaling mode.
//
// Any other query parameters are passed to SQLite verbatim.
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
		if mode := q.Get("journal"); mode != "" {
			opts.JournalMode = q.Get("journal")
			delete(q, "journal")
		}
		addr = base
		if r := q.Encode(); r != "" {
			addr += "?" + r
		}
	}

	return New(addr, &opts)
}

type Store struct {
	*monitor.M[*sqlDB, KV]
}

// Close implements part of the [blob.StoreCloser] interface.
func (s Store) Close(ctx context.Context) error {
	s.DB.txmu.Lock()
	defer s.DB.txmu.Unlock()

	// Attempt to truncate the WAL before closing.
	_, terr := s.DB.db.Exec(`pragma wal_checkpoint(TRUNCATE)`)

	// Attempt to vacuum the database before closing.
	_, verr := s.DB.db.Exec(`vacuum`)

	// Even if that fails, however, make sure the pool gets cleaned up.
	cerr := s.DB.db.Close()
	return errors.Join(terr, verr, cerr)
}

type sqlDB struct {
	// These fields are read-only after initialization.
	compress bool

	txmu sync.RWMutex // ex: write db, sh: read db
	db   *sql.DB
}

// A KV implements the [blob.KV] interface using a SQLite3 database.
type KV struct {
	db        *sqlDB
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
	if mode := opts.journalMode(); mode != "" {
		r := db.QueryRow("pragma journal_mode=" + mode)
		var gotMode string
		if err := r.Scan(&gotMode); err != nil {
			return Store{}, fmt.Errorf("set journal mode: %w", err)
		} else if gotMode != mode {
			return Store{}, fmt.Errorf("invalid journal mode %q", mode)
		}
	}
	return Store{M: monitor.New(monitor.Config[*sqlDB, KV]{
		DB: &sqlDB{db: db, compress: opts == nil || !opts.Uncompressed},
		NewKV: func(ctx context.Context, db *sqlDB, pfx dbkey.Prefix, _ string) (KV, error) {
			ktab := pfx.String() // hex-encoded

			db.txmu.Lock()
			defer db.txmu.Unlock()
			if err := withTxErr(ctx, db.db, func(tx *sql.Tx) error {
				_, err := tx.ExecContext(ctx, fmt.Sprintf(`create table if not exists "%s" (
  key BLOB primary key,
  value BLOB not null,
  vsize INTEGER not null
) without rowid`, ktab))
				return err
			}); err != nil {
				return KV{}, err
			}
			return KV{db: db, tableName: ktab}, nil
		},
	})}, nil
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

	// If set, set the journal mode of the database to this value.
	// See: https://sqlite.org/pragma.html#pragma_journal_mode
	JournalMode string
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

func (o *Options) journalMode() string {
	if o == nil {
		return ""
	}
	return o.JournalMode
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
		const sqliteConstraintUnique = 1555
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
func (s KV) List(ctx context.Context, start string) iter.Seq2[string, error] {
	return func(yield func(string, error) bool) {
		s.db.txmu.RLock()
		defer s.db.txmu.RUnlock()

		query := fmt.Sprintf(`select key from "%s" where key >= $start order by key`, s.tableName)
		if err := withTxErr(ctx, s.db.db, func(tx *sql.Tx) error {
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
				if !yield(skey, nil) {
					return nil // all done
				}
			}
			return rows.Close()
		}); err != nil {
			yield("", err)
		}
	}
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
