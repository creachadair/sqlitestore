// Copyright 2020 Michael J. Fromberger. All Rights Reserved.

package sqlitestore_test

import (
	"testing"

	"github.com/creachadair/ffs/blob/storetest"
	"github.com/creachadair/sqlitestore"
)

func TestStore(t *testing.T) {
	// N.B. We need cache=shared to support multiple connections on :memory:
	const dbURL = "file::memory:?cache=shared"

	t.Run("Uncompressed", func(t *testing.T) {
		db, err := sqlitestore.New(dbURL, &sqlitestore.Options{
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
		db, err := sqlitestore.New(dbURL, &sqlitestore.Options{
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
