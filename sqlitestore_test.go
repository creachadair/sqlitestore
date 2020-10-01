// Copyright 2020 Michael J. Fromberger. All Rights Reserved.

package sqlitestore_test

import (
	"testing"

	"github.com/creachadair/ffs/blob/storetest"
	"github.com/creachadair/sqlitestore"
)

func TestStore(t *testing.T) {
	// N.B. We need cache=shared to support multiple connections on :memory:
	db, err := sqlitestore.New("file::memory:?cache=shared", &sqlitestore.Options{
		PoolSize: 4,
		Table:    "testblobs",
	})
	if err != nil {
		t.Fatalf("New failed: %v", err)
	}

	storetest.Run(t, db)
}
