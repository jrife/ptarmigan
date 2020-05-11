package kv

import (
	"github.com/jrife/ptarmigan/storage/kv/keys"
)

// Namespace ensures that all keys referenced within a transaction
// are prefixed with ns.
func Namespace(txn Transaction, ns []byte) Transaction {
	return &namespacedTxn{txn: txn, ns: ns}
}

type namespacedTxn struct {
	txn Transaction
	ns  []byte
}

func (nsTxn *namespacedTxn) key(key []byte) []byte {
	return keys.All().Namespace(nsTxn.ns).Eq(key).Min
}

func (nsTxn *namespacedTxn) Put(key, value []byte) error {
	return nsTxn.txn.Put(nsTxn.key(key), value)
}

func (nsTxn *namespacedTxn) Get(key []byte) ([]byte, error) {
	return nsTxn.txn.Get(nsTxn.key(key))
}

func (nsTxn *namespacedTxn) Delete(key []byte) error {
	return nsTxn.txn.Delete(nsTxn.key(key))
}

func (nsTxn *namespacedTxn) Metadata() ([]byte, error) {
	return nsTxn.txn.Metadata()
}

func (nsTxn *namespacedTxn) SetMetadata(metadata []byte) error {
	return nsTxn.txn.SetMetadata(metadata)
}

func (nsTxn *namespacedTxn) Keys(keys keys.Range, order SortOrder) (Iterator, error) {
	if order != SortOrderAsc && order != SortOrderDesc {
		order = SortOrderAsc
	}

	iterator, err := nsTxn.txn.Keys(keys.Namespace(nsTxn.ns), order)

	if err != nil {
		return nil, err
	}

	return &namespacedIterator{iterator: iterator, ns: nsTxn.ns}, nil
}

func (nsTxn *namespacedTxn) Commit() error {
	return nsTxn.txn.Commit()
}

func (nsTxn *namespacedTxn) Rollback() error {
	return nsTxn.txn.Rollback()
}

type namespacedIterator struct {
	iterator Iterator
	key      []byte
	ns       []byte
}

func (nsCursor *namespacedIterator) Next() bool {
	if !nsCursor.iterator.Next() {
		nsCursor.key = nil

		return false
	}

	// strip the namespace prefix
	nsCursor.key = nsCursor.iterator.Key()[len(nsCursor.ns):]

	return true
}

func (nsCursor *namespacedIterator) Key() []byte {
	return nsCursor.key
}

func (nsCursor *namespacedIterator) Value() []byte {
	return nsCursor.iterator.Value()
}

func (nsCursor *namespacedIterator) Error() error {
	return nsCursor.iterator.Error()
}
