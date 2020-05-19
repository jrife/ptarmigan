package keys

import (
	"encoding/binary"

	"github.com/jrife/ptarmigan/utils/sequence"
)

// Int64ToKey constructs a key from an
// int64
func Int64ToKey(i int64) [8]byte {
	var k [8]byte

	binary.BigEndian.PutUint64(k[:], uint64(i))

	return k
}

// KeyToInt64 constructs an int64 from a
// byte array
func KeyToInt64(k [8]byte) int64 {
	return int64(binary.BigEndian.Uint64(k[:]))
}

type byteElement byte

func (b byteElement) Next() sequence.Element {
	if b == 0xff {
		return nil
	}

	return b + 1
}

func (b byteElement) Compare(e sequence.Element) int {
	if b < e.(byteElement) {
		return -1
	} else if b > e.(byteElement) {
		return 1
	}

	return 0
}

var _ sequence.Interface = (Key)(nil)
var _ sequence.Element = (Key)(nil)

// Key is a single key
type Key []byte

func (key Key) Len() int {
	return len(key)
}

func (key Key) Get(i int) sequence.Element {
	return byteElement(key[i])
}

func (key Key) Set(i int, e sequence.Element) {
	key[i] = byte(e.(byteElement))
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
	return append(key, byte(0))
}

func (key Key) Next() sequence.Element {
	return sequence.Next(key).(Key)
}

func (key Key) Compare(e sequence.Element) int {
	return sequence.Compare(key, e.(Key))
}

func Compare(a, b Key) int {
	return a.Compare(b)
}
