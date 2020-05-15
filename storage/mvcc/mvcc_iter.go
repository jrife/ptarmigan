package mvcc

import (
	"bytes"
	"fmt"

	"github.com/jrife/ptarmigan/storage/kv"
	"github.com/jrife/ptarmigan/storage/kv/keys"
)

type mvccIterator struct {
	iter       kv.Iterator
	parseKey   func(k []byte) ([]byte, int64, error)
	parseValue func(v []byte) ([]byte, error)
	k          []byte
	rev        int64
	v          []byte
	err        error
	done       bool
}

// advance and validate for format but without the internal consistency checks
func (iter *mvccIterator) n() (bool, []byte, []byte, int64, error) {
	if !iter.iter.Next() {
		return false, nil, nil, 0, iter.iter.Error()
	}

	k, revision, err := iter.parseKey(iter.iter.Key())

	if err != nil {
		return false, nil, nil, 0, fmt.Errorf("could not parse key %#v: %s", iter.iter.Key(), err)
	}

	if revision <= 0 {
		return false, nil, nil, 0, fmt.Errorf("consistency violation: revision number must be positive")
	}

	v, err := iter.parseValue(iter.iter.Value())

	if err != nil {
		return false, nil, nil, 0, fmt.Errorf("could not parse value %#v: %s", iter.iter.Value(), err)
	}

	return true, k, v, revision, nil
}

func (iter *mvccIterator) next() bool {
	if iter.done {
		return false
	}

	cont, key, value, revision, err := iter.n()

	if !cont {
		iter.done = true
		iter.err = err
		iter.k = nil
		iter.v = nil
		iter.rev = 0

		return false
	}

	iter.k = key
	iter.v = value
	iter.rev = revision

	return true
}

func (iter *mvccIterator) key() []byte {
	return iter.k
}

func (iter *mvccIterator) value() []byte {
	return iter.v
}

func (iter *mvccIterator) revision() int64 {
	return iter.rev
}

func (iter *mvccIterator) error() error {
	return iter.err
}

type revisionsCursor struct {
	revision int64
	key      []byte
}

func (c *revisionsCursor) bytes() revisionsKey {
	if c == nil {
		return nil
	}

	return newRevisionsKey(c.revision, c.key)
}

type revisionsIterator struct {
	mvccIterator
	order kv.SortOrder
}

func newRevisionsIterator(txn kv.Transaction, min *revisionsCursor, max *revisionsCursor, order kv.SortOrder) (*revisionsIterator, error) {
	iter, err := txn.Keys(keys.Range{Min: min.bytes(), Max: max.bytes()}, order)

	if err != nil {
		return nil, fmt.Errorf("could not create iterator: %s", err)
	}

	return &revisionsIterator{
		mvccIterator: mvccIterator{
			iter: iter,
			parseKey: func(key []byte) ([]byte, int64, error) {
				k, err := revisionsKey(key).key()

				if err != nil {
					return nil, 0, fmt.Errorf("could not split key and revision in %#v: %s", key, err)
				}

				rev, err := revisionsKey(key).revision()

				if err != nil {
					return nil, 0, fmt.Errorf("could not split key and revision in %#v: %s", key, err)
				}

				return k, rev, nil
			},
			parseValue: func(value []byte) ([]byte, error) {
				return revisionsValue(value).value(), nil
			},
		}, order: order,
	}, nil
}

func (iter *revisionsIterator) next() bool {
	oldRev := iter.rev

	if !iter.mvccIterator.next() {
		return false
	}

	defer func() {
		if iter.err != nil {
			iter.done = true
		}

		if iter.done {
			iter.k = nil
			iter.v = nil
			iter.rev = 0
		}
	}()

	if oldRev != 0 && oldRev != iter.rev {
		if iter.order == kv.SortOrderDesc && oldRev != iter.rev+1 {
			iter.err = fmt.Errorf("consistency violation: each revision must be exactly one less than the last: revision %d follows revision %d", iter.rev, oldRev)
			return false
		} else if oldRev != iter.rev-1 {
			iter.err = fmt.Errorf("consistency violation: each revision must be exactly one more than the last: revision %d follows revision %d", iter.rev, oldRev)
			return false
		}
	}

	return true
}

type keysCursor struct {
	revision int64
	key      []byte
}

func (c *keysCursor) bytes() keysKey {
	if c == nil {
		return nil
	}

	return newKeysKey(c.key, c.revision)
}

type keysIterator struct {
	mvccIterator
}

func newKeysIterator(txn kv.Transaction, min *keysCursor, max *keysCursor, order kv.SortOrder) (*keysIterator, error) {
	iter, err := txn.Keys(keys.Range{Min: min.bytes(), Max: max.bytes()}, kv.SortOrder(order))

	if err != nil {
		return nil, fmt.Errorf("could not create iterator: %s", err)
	}

	return &keysIterator{
		mvccIterator{
			iter: iter,
			parseKey: func(key []byte) ([]byte, int64, error) {
				k, err := keysKey(key).key()

				if err != nil {
					return nil, 0, fmt.Errorf("could not split key and revision in %#v: %s", key, err)
				}

				rev, err := keysKey(key).revision()

				if err != nil {
					return nil, 0, fmt.Errorf("could not split key and revision in %#v: %s", key, err)
				}

				return k, rev, nil
			},
			parseValue: func(value []byte) ([]byte, error) {
				return keysValue(value).value(), nil
			},
		},
	}, nil
}

var _ kv.Iterator = (*viewRevisionKeysIterator)(nil)

// Like keysIterator but it skips anything
type viewRevisionKeysIterator struct {
	keysIterator
	viewRevision int64
	currentKey   []byte
	currentValue []byte
	currentRev   int64
}

func newViewRevisionsIterator(txn kv.Transaction, min []byte, max []byte, revision int64, order kv.SortOrder) (*viewRevisionKeysIterator, error) {
	keysIterator, err := newKeysIterator(txn, &keysCursor{key: min}, &keysCursor{key: max}, order)

	if err != nil {
		return nil, err
	}

	// advance to the first position (if any)
	keysIterator.next()

	return &viewRevisionKeysIterator{keysIterator: *keysIterator, viewRevision: revision}, nil
}

// return the highest revision of the current key whose revision <= iter.viewRevision
func (iter *viewRevisionKeysIterator) highestRevision() bool {
	if iter.k == nil {
		return false
	}

	iter.currentKey = iter.k
	iter.currentRev = iter.rev
	iter.currentValue = iter.v

	for hasMore := true; hasMore && bytes.Compare(iter.currentKey, iter.k) == 0; hasMore = iter.keysIterator.next() {
		if iter.rev > iter.currentRev && iter.rev <= iter.viewRevision {
			iter.currentRev = iter.rev
			iter.currentValue = iter.v
		}
	}

	return true
}

// return the highest revision of each key whose revision <= iter.viewRevision
func (iter *viewRevisionKeysIterator) next() bool {
	if iter.k == nil {
		return false
	}

	hasMore := true

	// skips keys that don't have a revision <= iter.viewRevision
	// and keys whose most recent revision is "key deleted"
	for hasMore = iter.highestRevision(); hasMore && (iter.currentRev > iter.viewRevision || iter.currentValue == nil); hasMore = iter.highestRevision() {
	}

	return hasMore
}

// Some public functions to implement Iterator interface
func (iter *viewRevisionKeysIterator) Next() bool {
	return iter.next()
}

func (iter *viewRevisionKeysIterator) Key() []byte {
	return iter.key()
}

func (iter *viewRevisionKeysIterator) Value() []byte {
	return iter.value()
}

func (iter *viewRevisionKeysIterator) Error() error {
	return iter.error()
}

func (iter *viewRevisionKeysIterator) key() []byte {
	return iter.currentKey
}

func (iter *viewRevisionKeysIterator) value() []byte {
	return iter.currentValue
}

func (iter *viewRevisionKeysIterator) revision() int64 {
	return iter.currentRev
}

var _ DiffIterator = (*viewRevisionDiffsIterator)(nil)

type viewRevisionDiffsIterator struct {
	revisionsIterator
	viewRevision int64
	currentKey   []byte
	currentValue []byte
	currentRev   int64
}

func newViewRevisionDiffsIterator(txn kv.Transaction, min []byte, max []byte, revision int64) (*viewRevisionDiffsIterator, error) {
	iter, err := newRevisionsIterator(txn, &revisionsCursor{revision: revision}, &revisionsCursor{revision: revision, key: max}, kv.SortOrderAsc)

	if err != nil {
		return nil, fmt.Errorf("could not create revisions iterator: %s", err)
	}

	// Read empty revision marker first to get to the actual data
	if !iter.next() {
		if iter.error() != nil {
			return nil, fmt.Errorf("iteration error: %s", err)
		}

		// There is no revision marker for this revision so it must have been
		// compacted
		return nil, fmt.Errorf("consistency violation: revision %d should exist, but a record of it was not found", revision)
	}

	if iter.key() != nil {
		return nil, fmt.Errorf("consistency violation: the first key of each revision should be nil")
	}

	// Skip to the start key
	for hasMore := iter.next(); hasMore && bytes.Compare(iter.key(), min) < 0; hasMore = !iter.next() {
	}

	if iter.error() != nil {
		return nil, fmt.Errorf("iteration error: %s", err)
	}

	return &viewRevisionDiffsIterator{
		revisionsIterator: *iter,
		viewRevision:      revision,
	}, nil
}

// Some public functions to implement Iterator interface
func (iter *viewRevisionDiffsIterator) Next() bool {
	if iter.k == nil {
		return false
	}

	iter.currentKey = iter.k
	iter.currentValue = iter.v

	iter.next()

	return true
}

func (iter *viewRevisionDiffsIterator) IsDelete() bool {
	return iter.currentValue == nil
}

func (iter *viewRevisionDiffsIterator) IsPut() bool {
	return !iter.IsDelete()
}

func (iter *viewRevisionDiffsIterator) Key() []byte {
	return iter.currentKey
}

func (iter *viewRevisionDiffsIterator) Value() []byte {
	return iter.currentValue
}

func (iter *viewRevisionDiffsIterator) Prev() []byte {
	return nil
}

func (iter *viewRevisionDiffsIterator) Error() error {
	return iter.error()
}
