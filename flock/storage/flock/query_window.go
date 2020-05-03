package flock

import (
	"github.com/jrife/ptarmigan/storage/mvcc"
	"github.com/jrife/ptarmigan/utils/sorted_window"
)

var _ mvcc.UnmarshaledIterator = (*SortedIterator)(nil)

type SortedIterator struct {
	mvcc.UnmarshaledIterator
	window sorted_window.SortedWindow
}

func Sort(kvs mvcc.UnmarshaledIterator, after interface{}, limit int) mvcc.UnmarshaledIterator {
	iter := &SortedIterator{
		UnmarshaledIterator: kvs,
	}

	iter.sort()

	return iter
}

func (iter *SortedIterator) sort() {
	for iter.UnmarshaledIterator.Next() {
		iter.window.Push(iter.Value())
	}
}

func (iter *SortedIterator) Next() bool {
	if iter.window.Size() == 0 {
		return false
	}

	kv := iter.window.Pop()

	return false
}
