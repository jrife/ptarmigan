package flock

import (
	"fmt"

	"github.com/jrife/ptarmigan/flock/server/flockpb"
	"github.com/jrife/ptarmigan/storage/kv"
	"github.com/jrife/ptarmigan/storage/mvcc"
)

type cursor struct {
	mvcc.Marshalable
	compare func(k []byte) []byte
}

func (c cursor) Marshal() ([]byte, error) {
	key, err := c.Marshalable.Marshal()

	if err != nil {
		return nil, err
	}

	return c.compare(key[:]), nil
}

func gt(marshalable mvcc.Marshalable) mvcc.Marshalable {
	return &cursor{marshalable, kv.Gt}
}

func gte(marshalable mvcc.Marshalable) mvcc.Marshalable {
	return &cursor{marshalable, kv.Gte}
}

func lt(marshalable mvcc.Marshalable) mvcc.Marshalable {
	return &cursor{marshalable, kv.Lt}
}

func lte(marshalable mvcc.Marshalable) mvcc.Marshalable {
	return &cursor{marshalable, kv.Lte}
}

func prefixRangeStart(marshalable mvcc.Marshalable) mvcc.Marshalable {
	return nil
}

func prefixRangeEnd(marshalable mvcc.Marshalable) mvcc.Marshalable {
	return nil
}

type leasesKey int64

func (k leasesKey) Marshal() ([]byte, error) {
	b := kv.Int64ToKey(int64(k))

	return b[:], nil
}

func unmarshalLease(b []byte) (interface{}, error) {
	var lease flockpb.Lease

	if err := lease.Unmarshal(b); err != nil {
		return nil, err
	}

	return lease, nil
}

func unmarshalLeaseKey(b []byte) (interface{}, error) {
	if len(b) != 8 {
		return nil, fmt.Errorf("key is not 8 bytes")
	}

	var arr [8]byte
	copy(arr[:], b[:8])

	return kv.KeyToInt64(arr), nil
}

func leasesView(view mvcc.View) *mvcc.MarshalingView {
	return mvcc.NewMarshalingView(view, unmarshalLeaseKey, unmarshalLease)
}

func leasesRevision(revision mvcc.Revision) *mvcc.MarshalingRevision {
	return mvcc.NewMarshalingRevision(revision, unmarshalLeaseKey, unmarshalLease)
}

func leases(kvs []mvcc.UnmarshaledKV) []flockpb.Lease {
	leases := make([]flockpb.Lease, len(kvs))

	for i, kv := range kvs {
		leases[i] = kv[i].(flockpb.Lease)
	}

	return leases
}

type kvsKey []byte

func (k kvsKey) Marshal() ([]byte, error) {
	return k, nil
}

func unmarshalKV(b []byte) (interface{}, error) {
	var kv flockpb.KeyValue

	if err := kv.Unmarshal(b); err != nil {
		return nil, err
	}

	return kv, nil
}

func unmarshalKVKey(b []byte) (interface{}, error) {
	return b, nil
}

func kvsView(view mvcc.View) *mvcc.MarshalingView {
	return nil
}

func kvsRevision(revision mvcc.Revision) *mvcc.MarshalingRevision {
	return nil
}

func keyValues(kvs []mvcc.UnmarshaledKV) []flockpb.KeyValue {
	keyValues := make([]flockpb.KeyValue, len(kvs))

	for i, kv := range kvs {
		keyValues[i] = kv[i].(flockpb.KeyValue)
	}

	return keyValues
}
