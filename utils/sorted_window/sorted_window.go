package sorted_window

import (
	"github.com/emirpasic/gods/sets/treeset"
	"github.com/emirpasic/gods/utils"
)

type SortedWindow interface {
	Insert(obj interface{})
	Iterator() Iterator
	Size() int
}

type Iterator interface {
	Next() bool
	Value() interface{}
}

type Comparator func(a, b interface{}) int

type option func(*sortedWindow)

func WithLimit(l int) option {
	return func(sortedWindow *sortedWindow) {
		sortedWindow.limit = l
	}
}

type sortedWindow struct {
	set     *treeset.Set
	compare Comparator
	max     interface{}
	limit   int
}

func New(comparator Comparator, opts ...option) SortedWindow {
	sw := &sortedWindow{
		set:     treeset.NewWith(utils.Comparator(comparator)),
		compare: comparator,
		limit:   -1,
	}

	for _, opt := range opts {
		opt(sw)
	}

	return sw
}

func (sortedWindow *sortedWindow) Insert(obj interface{}) {
	if sortedWindow.Size() == sortedWindow.limit && sortedWindow.max != nil && sortedWindow.compare(obj, sortedWindow.max) < 0 {
	}

	sortedWindow.set.Add(obj)
}

func (sortedWindow *sortedWindow) Iterator() Iterator {
	return nil
}

func (sortedWindow *sortedWindow) Size() int {
	return sortedWindow.set.Size()
}
