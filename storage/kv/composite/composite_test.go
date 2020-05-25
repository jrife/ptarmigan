package composite_test

import (
	"bytes"
	"errors"
	"sort"
	"testing"

	"github.com/google/go-cmp/cmp"
	"github.com/jrife/ptarmigan/storage/kv"
	"github.com/jrife/ptarmigan/storage/kv/composite"
	"github.com/jrife/ptarmigan/storage/kv/keys"
	composite_keys "github.com/jrife/ptarmigan/storage/kv/keys/composite"
)

func ids() func() int64 {
	var n int64

	return func() int64 {
		n++
		return n
	}
}

func TestMapPut(t *testing.T) {
	testCases := map[string]struct {
		key   composite_keys.Key
		value []byte
		err   error
	}{
		"empty-key-non-empty-value": {
			key:   composite_keys.Key{},
			value: []byte("abc"),
			err:   errors.New(""),
		},
		"nil-key-non-empty-value": {
			key:   nil,
			value: []byte("abc"),
			err:   errors.New(""),
		},
		"non-empty-key-non-empty-value": {
			key:   composite_keys.Key{[]byte("a")},
			value: []byte("abc"),
		},
		"non-empty-key-empty-value": {
			key:   composite_keys.Key{[]byte("a")},
			value: []byte{},
		},
		"non-empty-key-nil-value": {
			key:   composite_keys.Key{[]byte("a")},
			value: nil,
			err:   errors.New(""),
		},
		"nil-key-nil-value": {
			key:   nil,
			value: nil,
			err:   errors.New(""),
		},
		"nil-key-element": {
			key:   composite_keys.Key{nil},
			value: nil,
			err:   errors.New(""),
		},
		"empty-key-elements": {
			key:   composite_keys.Key{[]byte{}},
			value: nil,
			err:   errors.New(""),
		},
	}

	for name, testCase := range testCases {
		t.Run(name, func(t *testing.T) {
			cm := composite.NewMap(kv.NewFakeMap(), ids())
			err := cm.Put(testCase.key, testCase.value)

			// does err match expected error
			if testCase.err != nil {
				if err == nil {
					t.Fatalf("expected err to not be nil, got nil")
				}
			} else if err != nil {
				if err == nil {
					t.Fatalf("expected err to be nil, got %#v", err)
				}
			}
		})
	}
}

func TestMapDelete(t *testing.T) {
	testCases := map[string]struct {
		key composite_keys.Key
		err error
	}{
		"empty-key": {
			key: composite_keys.Key{},
			err: errors.New(""),
		},
		"nil-key": {
			key: nil,
			err: errors.New(""),
		},
		"non-empty-key": {
			key: composite_keys.Key{[]byte("a")},
		},
		"nil-key-element": {
			key: composite_keys.Key{nil},
			err: errors.New(""),
		},
		"empty-key-elements": {
			key: composite_keys.Key{[]byte{}},
			err: errors.New(""),
		},
	}

	for name, testCase := range testCases {
		t.Run(name, func(t *testing.T) {
			cm := composite.NewMap(kv.NewFakeMap(), ids())
			err := cm.Delete(testCase.key)

			// does err match expected error
			if testCase.err != nil {
				if err == nil {
					t.Fatalf("expected err to not be nil, got nil")
				}
			} else if err != nil {
				if err == nil {
					t.Fatalf("expected err to be nil, got %#v", err)
				}
			}
		})
	}
}

type op struct {
	Key   composite_keys.Key
	Value []byte
}

type ops []op

func (o ops) Len() int {
	return len(o)
}

func (o ops) Less(i, j int) bool {
	cmp := composite_keys.Compare(o[i].Key, o[j].Key)

	return cmp < 0 || (cmp == 0 && i < j)
}

func (o ops) Swap(i, j int) {
	o[i], o[j] = o[j], o[i]
}

func (o ops) Get(k composite_keys.Key) []byte {
	for _, op := range o {
		if composite_keys.Compare(op.Key, k) == 0 {
			return op.Value
		}
	}

	return nil
}

func (o ops) SortAndDeduplicate() ops {
	if len(o) == 0 {
		return o
	}

	// copy
	o = append(ops(nil), o...)

	sort.Sort(o)

	i := 0

	for j := 1; j < len(o); j++ {
		if composite_keys.Compare(o[i].Key, o[j].Key) != 0 {
			i++
		}

		o[i] = o[j]
	}

	return o[:i+1]
}

func (o ops) Range(keys composite_keys.Range, order kv.SortOrder) ops {
	slice := ops{}

	for _, op := range o {
		if keys.Contains(op.Key) {
			slice = append(slice, op)
		}
	}

	if order == kv.SortOrderDesc {
		slice = sort.Reverse(slice).(ops)
	}

	return slice
}

var aBitOfEverything = []op{
	{
		Key:   [][]byte{[]byte("xxx"), []byte("yyy"), []byte("zzz")},
		Value: []byte("xyz"),
	},
	{
		Key:   [][]byte{[]byte("a"), []byte("b")},
		Value: []byte("abkey"),
	},
	{
		Key:   [][]byte{[]byte("a")},
		Value: []byte("akey"),
	},
	{
		Key:   [][]byte{[]byte("a"), []byte("b"), []byte("c")},
		Value: []byte("abckey"),
	},
	{
		Key:   [][]byte{[]byte("aa"), []byte("0000")},
		Value: []byte("aa0000"),
	},
	{
		Key:   [][]byte{[]byte("aa"), []byte("0001")},
		Value: []byte("aa0001"),
	},
	{
		Key:   [][]byte{[]byte("aa"), []byte("0002")},
		Value: []byte("aa0002"),
	},
	{
		Key:   [][]byte{[]byte("bb"), []byte("0000")},
		Value: []byte("bb0000"),
	},
	{
		Key:   [][]byte{[]byte("bb"), []byte("0001")},
		Value: []byte("bb0001"),
	},
	{
		Key:   [][]byte{[]byte("bb"), []byte("0002")},
		Value: []byte("bb0002"),
	},
	{
		Key:   [][]byte{[]byte("cc"), []byte("0000")},
		Value: []byte("cc0000"),
	},
	{
		Key:   [][]byte{[]byte("cc"), []byte("0001")},
		Value: []byte("cc0001"),
	},
	{
		Key:   [][]byte{[]byte("cc"), []byte("0002")},
		Value: []byte("cc0002"),
	},
}

func TestMapGet(t *testing.T) {
	testCases := map[string]struct {
		initialState []op
		key          composite_keys.Key
		err          error
	}{
		"empty-key": {
			initialState: aBitOfEverything,
			key:          composite_keys.Key{},
			err:          errors.New(""),
		},
		"nil-key": {
			initialState: aBitOfEverything,
			key:          nil,
			err:          errors.New(""),
		},
		"nil-key-element": {
			initialState: aBitOfEverything,
			key:          composite_keys.Key{nil},
			err:          errors.New(""),
		},
		"empty-key-elements": {
			initialState: aBitOfEverything,
			key:          composite_keys.Key{[]byte{}},
			err:          errors.New(""),
		},
		"non-existent-key-1": {
			initialState: []op{},
			key:          composite_keys.Key{[]byte("a")},
		},
		"non-existent-key-2": {
			initialState: []op{},
			key:          composite_keys.Key{[]byte("a"), []byte("b")},
		},
		"non-existent-key-3": {
			initialState: []op{},
			key:          composite_keys.Key{[]byte("a"), []byte("b"), []byte("c")},
		},
		"existent-key-1": {
			initialState: aBitOfEverything,
			key:          composite_keys.Key{[]byte("a")},
		},
		"existent-key-2": {
			initialState: aBitOfEverything,
			key:          composite_keys.Key{[]byte("a"), []byte("b")},
		},
		"existent-key-3": {
			initialState: aBitOfEverything,
			key:          composite_keys.Key{[]byte("a"), []byte("b"), []byte("c")},
		},
		"non-existent-key-4": {
			initialState: aBitOfEverything,
			key:          composite_keys.Key{[]byte("xxx")},
		},
		"non-existent-key-5": {
			initialState: aBitOfEverything,
			key:          composite_keys.Key{[]byte("xxx"), []byte("yyy")},
		},
		"non-existent-key-6": {
			initialState: aBitOfEverything,
			key:          composite_keys.Key{[]byte("xxx"), []byte("yyy"), []byte("zzz"), []byte("c")},
		},
	}

	for name, testCase := range testCases {
		t.Run(name, func(t *testing.T) {
			cm := composite.NewMap(kv.NewFakeMap(), ids())

			for _, op := range testCase.initialState {
				if op.Value == nil {
					cm.Delete(op.Key)
				} else {
					cm.Put(op.Key, op.Value)
				}
			}

			value, err := cm.Get(testCase.key)

			// does err match expected error
			if testCase.err != nil {
				if err == nil {
					t.Fatalf("expected err to not be nil, got nil")
				}
			} else if err != nil {
				if err == nil {
					t.Fatalf("expected err to be nil, got %#v", err)
				}
			}

			expectedValue := ops(testCase.initialState).SortAndDeduplicate().Get(testCase.key)

			if bytes.Compare(expectedValue, value) != 0 {
				t.Fatalf("expected value to be %#v, got %#v", expectedValue, value)
			}
		})
	}
}

func TestMapKeys(t *testing.T) {
	testCases := map[string]struct {
		initialState []op
		keys         composite_keys.Range
		order        kv.SortOrder
	}{
		"aa-bb+0001-": {
			initialState: aBitOfEverything,
			keys:         composite_keys.Range{keys.All().Gte([]byte("aa")).Lte([]byte("bb")), keys.All().Gte([]byte("0001"))},
			order:        kv.SortOrderAsc,
		},
	}

	for name, testCase := range testCases {
		t.Run(name, func(t *testing.T) {
			cm := composite.NewMap(kv.NewFakeMap(), ids())

			for _, op := range testCase.initialState {
				if op.Value == nil {
					cm.Delete(op.Key)
				} else {
					cm.Put(op.Key, op.Value)
				}
			}

			iter, err := cm.Keys(testCase.keys, testCase.order)

			if err != nil {
				t.Fatalf("expected err to be nil, got %#v", err)
			}

			expectedKeys := ops(testCase.initialState).SortAndDeduplicate().Range(testCase.keys, testCase.order)
			actualKeys := ops{}

			for iter.Next() {
				actualKeys = append(actualKeys, op{Key: append(composite_keys.Key(nil), iter.Key()...), Value: append([]byte(nil), iter.Value()...)})
			}

			if iter.Error() != nil {
				t.Fatalf("expected err to be nil, got %#v", iter.Error())
			}

			diff := cmp.Diff(expectedKeys, actualKeys)

			if diff != "" {
				t.Fatalf(diff)
			}
		})
	}
}
