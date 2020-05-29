package keys

import (
	"bytes"
	"encoding/binary"
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

// Key is a single key
type Key []byte

// Compare compares two keys
// -1 means a < b
// 1 means a > b
// 0 means a = b
func Compare(a, b Key) int {
	return bytes.Compare(a, b)
}

// Inc increments the key
func Inc(key Key) Key {
	carry := true
	after := make(Key, len(key))

	copy(after, key)

	for i := len(after) - 1; i >= 0 && carry; i-- {
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

func Next(key Key) Key {
	return append(key, 0)
}
