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

// Len implements sequence.Interface.Len
func (key Key) Len() int {
	return len(key)
}

// Get implements sequence.Interface.Get
func (key Key) Get(i int) sequence.Element {
	return byteElement(key[i])
}

// Set implements sequence.Interface.Set
func (key Key) Set(i int, e sequence.Element) {
	key[i] = byte(e.(byteElement))
}

// Inc implements sequence.Interface.Inc
func (key Key) Inc() sequence.Interface {
	return Inc(key)
}

// Copy implements sequence.Interface.Copy
func (key Key) Copy() sequence.Interface {
	cp := make(Key, len(key))

	copy(cp, key)

	return cp
}

// AppendMinElement implements sequence.Interface.AppendMinElement
func (key Key) AppendMinElement() sequence.Interface {
	return append(key, byte(0))
}

// Next implements sequence.Element.Next
func (key Key) Next() sequence.Element {
	return sequence.Next(key).(Key)
}

// Compare implements sequence.Element.Compare
func (key Key) Compare(e sequence.Element) int {
	return sequence.Compare(key, e.(Key))
}

// Compare compares two keys
// -1 means a < b
// 1 means a > b
// 0 means a = b
func Compare(a, b Key) int {
	return a.Compare(b)
}

// Inc increments the key
func Inc(key Key) Key {
	carry := true
	after := make(Key, len(key))

	for i := after.Len() - 1; i >= 0 && carry; i-- {
		if key[i] < 0xff {
			carry = false
		}

		after[i] = key[i] + 1
	}

	// carry will only be true if all elements of k
	// were equal to 0xff. The range should just go
	// all the way to the end of the real key range.
	if carry {
		return nil
	}

	return after
}
