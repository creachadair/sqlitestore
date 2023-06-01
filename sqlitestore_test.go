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
			Table:        "testblobs",
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
			Table:        "packblobs",
			Uncompressed: false,
		})
		if err != nil {
			t.Fatalf("New failed: %v", err)
		}
		storetest.Run(t, db)
	})
}
