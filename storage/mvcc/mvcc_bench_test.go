package mvcc_test

import (
	"fmt"
	"testing"

	"github.com/jrife/ptarmigan/storage/kv/plugins"
)

func BenchmarkMVCC(b *testing.B) {
	for _, plugin := range plugins.Plugins() {
		b.Run(fmt.Sprintf("BenchmarkMVCC(%s)", plugin.Name()), benchMVCC(builder(plugin)))
	}
}

func benchMVCC(builder tempStoreBuilder) func(b *testing.B) {
	return func(b *testing.B) {
		benchDriver(builder, b)
	}
}

func benchDriver(builder tempStoreBuilder, b *testing.B) {
	b.Run("Writes", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			bench100000Writes(builder, b)
		}
	})
}

func bench100000Writes(builder tempStoreBuilder, b *testing.B) {
	store := builder(b, storeChangeset{})
	defer store.Delete()
	partitionA := store.Partition([]byte("a"))
	err := partitionA.Create([]byte{})

	if err != nil {
		b.Fatalf("expected err to be nil, got %#v", err)
	}

	for i := 0; i < 10000; i++ {
		txn, err := partitionA.Begin(true)

		if err != nil {
			b.Fatalf("expected err to be nil, got %#v", err)
		}

		revision, err := txn.NewRevision()

		if err != nil {
			txn.Rollback()
			b.Fatalf("expected err to be nil, got %#v", err)
		}

		err = revision.Put([]byte("key-1"), []byte(fmt.Sprintf("key1-value-%d", i)))
		if err != nil {
			txn.Rollback()
			b.Fatalf("expected err to be nil, got %#v", err)
		}
		err = revision.Put([]byte("key-2"), []byte(fmt.Sprintf("key2-value-%d", i)))
		if err != nil {
			txn.Rollback()
			b.Fatalf("expected err to be nil, got %#v", err)
		}
		err = revision.Put([]byte("key-3"), []byte(fmt.Sprintf("key3-value-%d", i)))
		if err != nil {
			txn.Rollback()
			b.Fatalf("expected err to be nil, got %#v", err)
		}
		err = txn.Commit()
		if err != nil {
			txn.Rollback()
			b.Fatalf("expected err to be nil, got %#v", err)
		}
	}
}
