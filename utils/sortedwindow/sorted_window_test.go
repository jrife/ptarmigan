package sortedwindow_test

import (
	"testing"

	"github.com/jrife/ptarmigan/utils/sortedwindow"
)

func TestSortedWindow(t *testing.T) {
	sw := sortedwindow.New(func(a, b interface{}) int {
		if a.(int) < b.(int) {
			return 1
		} else if a.(int) > b.(int) {
			return -1
		}

		return 0
	}, sortedwindow.WithLimit(1000))

	for i := 0; i < 100000; i++ {
		sw.Insert(i)
	}

	if sw.Size() != 1000 {
		t.Fatalf("Size should have been 1000")
	}

	i := 99999
	for iter := sw.Iterator(); iter.Next(); i-- {
		if iter.Value().(int) != i {
			t.Fatalf("expected value to be %d, got %d", i, iter.Value().(int))
		}
	}

	if i != 98999 {
		t.Fatalf("expected value to be 98999, got %d", i)
	}

	sw = sortedwindow.New(func(a, b interface{}) int {
		if a.(int) < b.(int) {
			return 1
		} else if a.(int) > b.(int) {
			return -1
		}

		return 0
	})

	for i := 0; i < 100000; i++ {
		sw.Insert(i)
	}

	if sw.Size() != 100000 {
		t.Fatalf("Size should have been 1000")
	}

	i = 99999
	for iter := sw.Iterator(); iter.Next(); i-- {
		if iter.Value().(int) != i {
			t.Fatalf("expected value to be %d, got %d", i, iter.Value().(int))
		}
	}

	if i != -1 {
		t.Fatalf("expected value to be 98999, got %d", i)
	}
}
