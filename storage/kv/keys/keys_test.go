package keys_test

import (
	"fmt"
	"testing"

	"github.com/jrife/ptarmigan/storage/kv/keys"
)

func TestKeys(t *testing.T) {
	fmt.Printf("%#v\n", keys.Key([]byte{0x04, 0xff}).Inc().(keys.Key))
}
