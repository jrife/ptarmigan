package mvcc_test

import (
	"testing"
)

func testView(builder tempStoreBuilder, t *testing.T) {
	t.Run("Keys", func(t *testing.T) { testViewKeys(builder, t) })
	t.Run("Changes", func(t *testing.T) { testViewChanges(builder, t) })
	t.Run("Revision", func(t *testing.T) { testViewRevision(builder, t) })
}

func testViewKeys(builder tempStoreBuilder, t *testing.T) {
}

func testViewChanges(builder tempStoreBuilder, t *testing.T) {
}

func testViewRevision(builder tempStoreBuilder, t *testing.T) {
}
