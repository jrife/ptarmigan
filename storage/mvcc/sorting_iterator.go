package mvcc

import (
	"github.com/jrife/ptarmigan/utils/sortedwindow"
)

func Sort(iter Iterator, compare func(a KV, b KV) int, limit int) Iterator {
	sortedIter := &unmarshaledSortingIterator{}

	window := sortedwindow.New(func(a, b interface{}) int {
		return compare(a.(KV), b.(KV))
	}, sortedwindow.WithLimit(limit))

	for iter.Next() {
		window.Insert(UnmarshaledKV{iter.Key(), iter.Value()})
	}

	sortedIter.iter = window.Iterator()

	return iter
}

type sortingIterator struct {
	Iterator
	iter *sortedwindow.Iterator
}

func (iter *sortingIterator) Next() bool {
	return iter.Next()
}

func (iter *sortingIterator) Key() interface{} {
	return iter.Value().(KV)[0]
}

func (iter *sortingIterator) Value() interface{} {
	return iter.Value().(KV)[1]
}

func SortMarshaled(iter UnmarshaledIterator, compare func(a UnmarshaledKV, b UnmarshaledKV) int, limit int) UnmarshaledIterator {
	sortedIter := &unmarshaledSortingIterator{}

	window := sortedwindow.New(func(a, b interface{}) int {
		return compare(a.(UnmarshaledKV), b.(UnmarshaledKV))
	}, sortedwindow.WithLimit(limit))

	for iter.Next() {
		window.Insert(UnmarshaledKV{iter.Key(), iter.Value()})
	}

	sortedIter.iter = window.Iterator()

	return sortedIter
}

type unmarshaledSortingIterator struct {
	UnmarshaledIterator
	iter *sortedwindow.Iterator
}

func (iter *unmarshaledSortingIterator) Next() bool {
	return iter.Next()
}

func (iter *unmarshaledSortingIterator) Key() interface{} {
	return iter.Value().(UnmarshaledKV)[0]
}

func (iter *unmarshaledSortingIterator) Value() interface{} {
	return iter.Value().(UnmarshaledKV)[1]
}
