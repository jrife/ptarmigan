package sortedwindow

import (
	"github.com/emirpasic/gods/utils"
	"github.com/jrife/ptarmigan/utils/sortedwindow/redblacktree"
)

const (
	defaultCapacity = 100
)

// Comparator is a function that compares two values
// return 0 if they are equal
// return -1 if a < b
// return 1 if a > b
type Comparator func(a, b interface{}) int

type option func(*SortedMinWindow)

// WithLimit sets the size limit for the
// sorted window.
func WithLimit(l int) option {
	return func(sortedWindow *SortedMinWindow) {
		sortedWindow.limit = l
	}
}

// SortedMinWindow is used to sort a stream of values
// when that stream may be too large to buffer in memory
// If limit is positive it sets the window size. If limit
// is negative or zero there is no limit and the window
// will grow to hold the entire data set in memory.
type SortedMinWindow struct {
	compare Comparator
	limit   int
	tree    *redblacktree.Tree
}

// New creates a new SortedMinWindow.
func New(comparator Comparator, opts ...option) *SortedMinWindow {
	sw := &SortedMinWindow{
		compare: comparator,
		limit:   -1,
	}

	for _, opt := range opts {
		opt(sw)
	}

	cap := defaultCapacity

	if sw.limit >= 0 {
		cap = sw.limit
	}

	sw.tree = redblacktree.NewWith(utils.Comparator(comparator), cap)

	return sw
}

// Insert attempts to insert obj into the window. If limit is <= 0
// it will be inserted. If limit > 0 and size has not yet hit limit then
// it will be inserted. If limit > 0 and size has hit limit and obj >=
// the max value in the window then it will not be inserted.
// If limit > 0 and size has hit limit and obj < the max value in
// the window then it will be inserted and the max value will be removed.
func (sortedWindow *SortedMinWindow) Insert(obj interface{}) {
	if sortedWindow.limit <= 0 || sortedWindow.tree.Size() < sortedWindow.limit {
		sortedWindow.tree.Put(obj, nil)
	} else if sortedWindow.limit > 0 && sortedWindow.compare(obj, sortedWindow.tree.Right().Key) < 0 {
		// Keep only the smallest N values that we see, where N = limit
		sortedWindow.tree.PutAndRecycleMax(obj, nil)
	}
}

// Iterator returns an iterator for the window that returns
// values in ascending order
func (sortedWindow *SortedMinWindow) Iterator() *Iterator {
	return &Iterator{iter: sortedWindow.tree.Iterator()}
}

// Size returns the number of elements in the window
func (sortedWindow *SortedMinWindow) Size() int {
	return sortedWindow.tree.Size()
}

// Iterator is an iterator for a sorted window
type Iterator struct {
	iter redblacktree.Iterator
}

// Next advances the iterator. It must be called
// once to advance to the first position. It returns
// true if there is another value available, false
// otherwise
func (iter *Iterator) Next() bool {
	return iter.iter.Next()
}

// Value returns the value at the current position
func (iter *Iterator) Value() interface{} {
	return iter.iter.Key()
}
