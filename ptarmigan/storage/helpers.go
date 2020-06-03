package storage

import (
	"github.com/jrife/flock/ptarmigan/server/ptarmiganpb"
	"github.com/jrife/flock/storage/kv"
	"github.com/jrife/flock/storage/kv/keys"
	kv_marshaled "github.com/jrife/flock/storage/kv/marshaled"
)

func leasesKey(id int64) []byte {
	k := keys.Int64ToKey(id)

	return k[:]
}

func leasesMapReader(m kv.MapReader) *kv_marshaled.MapReader {
	return &kv_marshaled.MapReader{
		MapReader: m,
		Unmarshal: unmarshalLease,
	}
}

func leasesMap(m kv.Map) *kv_marshaled.Map {
	return &kv_marshaled.Map{
		MapUpdater: kv_marshaled.MapUpdater{MapUpdater: m},
		MapReader:  *leasesMapReader(m),
	}
}

func unmarshalLease(b []byte) (interface{}, error) {
	var lease ptarmiganpb.Lease

	if err := lease.Unmarshal(b); err != nil {
		return nil, err
	}

	return lease, nil
}

func kvMapReader(m kv.MapReader) *kv_marshaled.MapReader {
	return &kv_marshaled.MapReader{
		MapReader: m,
		Unmarshal: unmarshalKV,
	}
}

func kvMap(m kv.Map) *kv_marshaled.Map {
	return &kv_marshaled.Map{
		MapUpdater: kv_marshaled.MapUpdater{MapUpdater: m},
		MapReader:  *kvMapReader(m),
	}
}

func unmarshalKV(b []byte) (interface{}, error) {
	if b == nil {
		return nil, nil
	}

	var kv ptarmiganpb.KeyValue

	if err := kv.Unmarshal(b); err != nil {
		return nil, err
	}

	return kv, nil
}
