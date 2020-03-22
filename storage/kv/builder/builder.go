package builder

import (
	"fmt"

	"github.com/jrife/ptarmigan/storage/kv"
	"github.com/jrife/ptarmigan/storage/kv/bbolt"
)

type KVStoreBuilder func(options map[string]interface{}) (kv.Store, error)

var Drivers map[string]KVStoreBuilder = map[string]KVStoreBuilder{
	"bbolt": bbolt.Builder,
}

func MakeStore(driver string, options map[string]interface{}) (kv.Store, error) {
	builder, ok := Drivers[driver]

	if !ok {
		return nil, fmt.Errorf("%s is not a valid driver", driver)
	}

	return builder(options)
}
