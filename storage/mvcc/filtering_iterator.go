package mvcc

func Filter(iter Iterator, filter func(key, value []byte) bool) Iterator {
	return &filteringIterator{iter, filter}
}

type filteringIterator struct {
	Iterator
	filter func(key, value []byte) bool
}

func (iter *filteringIterator) Next() bool {
	hasMore := false

	for hasMore = iter.Next(); hasMore && !iter.filter(iter.Key(), iter.Value()); hasMore = iter.Next() {
	}

	return hasMore
}

func FilterMarshaled(iter UnmarshaledIterator, filter func(key, value interface{}) bool) UnmarshaledIterator {
	return &unmarshaledFilteringIterator{iter, filter}
}

type unmarshaledFilteringIterator struct {
	UnmarshaledIterator
	filter func(key, value interface{}) bool
}

func (iter *unmarshaledFilteringIterator) Next() bool {
	hasMore := false

	for hasMore = iter.Next(); hasMore && !iter.filter(iter.Key(), iter.Value()); hasMore = iter.Next() {
	}

	return hasMore
}
