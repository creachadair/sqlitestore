// Copyright 2020 Michael J. Fromberger. All Rights Reserved.

package sqlitestore_test

import (
	"path/filepath"
	"testing"

	"github.com/creachadair/ffs/blob/storetest"
	"github.com/creachadair/sqlitestore"
)

func TestStore(t *testing.T) {
	t.Run("Uncompressed", func(t *testing.T) {
		url := "file:" + filepath.Join(t.TempDir(), "test.db")
		db, err := sqlitestore.New(url, &sqlitestore.Options{
			PoolSize:     4,
			Uncompressed: true,
		})
		if err != nil {
			t.Fatalf("New failed: %v", err)
		}
		storetest.Run(t, db)
	})

	t.Run("Compressed", func(t *testing.T) {
		url := "file:" + filepath.Join(t.TempDir(), "test.db")
		db, err := sqlitestore.New(url, &sqlitestore.Options{
			PoolSize:     4,
			Uncompressed: false,
		})
		if err != nil {
			t.Fatalf("New failed: %v", err)
		}
		storetest.Run(t, db)
	})

	t.Run("WAL", func(t *testing.T) {
		url := "file:" + filepath.Join(t.TempDir(), "test.db")
		db, err := sqlitestore.New(url, &sqlitestore.Options{
			JournalMode: "wal",
		})
		if err != nil {
			t.Fatalf("New failed; %v", err)
		}
		storetest.Run(t, db)
	})
}

func BenchmarkStore(b *testing.B) {
	url := "file:" + filepath.Join(b.TempDir(), "benchmark.db")
	db, err := sqlitestore.New(url, nil)
	if err != nil {
		b.Fatal(err)
	}
	kv, err := db.KV(b.Context(), "benchmark")
	if err != nil {
		b.Fatalf("KV: %v", err)
	}
	storetest.BenchmarkKV(b, kv)
}
