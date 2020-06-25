package keys_test

import (
	"fmt"
	"testing"

	"github.com/jrife/flock/storage/kv/keys"
)

func TestKeys(t *testing.T) {
	fmt.Printf("%#v\n", keys.Inc(keys.Key([]byte{0x04, 0xff})))
	fmt.Printf("%#v\n", keys.All().Prefix([]byte("bb")).Lt([]byte("a")))
}
