package keys

import (
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
