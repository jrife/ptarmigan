package composite

import (
	"github.com/jrife/ptarmigan/storage/kv/keys"
	"github.com/jrife/ptarmigan/utils/sequence"
)

var _ sequence.Interface = (Key)(nil)

// Key is a sequence of keys
type Key []keys.Key

func (key Key) Len() int {
	return len(key)
}

func (key Key) Get(i int) sequence.Element {
	return key[i]
}

func (key Key) Set(i int, e sequence.Element) {
	key[i] = e.(keys.Key)
}

func (key Key) Inc() sequence.Interface {
	return sequence.Inc(key)
}

func (key Key) Copy() sequence.Interface {
	cp := make(Key, len(key))

	copy(cp, key)

	return cp
}

func (key Key) AppendMinElement() sequence.Interface {
	return append(key, keys.Key{})
}

func Compare(a, b Key) int {
	return sequence.Compare(a, b)
}
