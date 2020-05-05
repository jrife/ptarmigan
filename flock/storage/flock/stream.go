package flock

import (
	"bytes"
	"encoding/binary"

	"github.com/jrife/ptarmigan/flock/server/flockpb"
	"github.com/jrife/ptarmigan/storage/mvcc"
	"github.com/jrife/ptarmigan/utils/stream"
)

func QueryPipeline(query flockpb.KVQueryRequest) []stream.Processor {
	// Parameters: SortTarget, SortOrder, Selection, After, Limit, ExcludeValues, IncludeCount
	// SortTarget = KEY     -> natural order, no need for full scan + windowing unless IncludeCount is set
	//   selection keys -> count
	//                  -> filter -> sort(limit)
	// SortTarget = VERSION -> requires full scan + windowing
	// SortTarget = CREATE  -> requires full scan + windowing
	// SortTarget = MOD     -> requires full scan + windowing
	// SortTarget = VALUE   -> requires full scan + windowing
	switch query.SortTarget {
	case flockpb.KVQueryRequest_KEY:
		return nil
	case flockpb.KVQueryRequest_VERSION:
	case flockpb.KVQueryRequest_CREATE:
	case flockpb.KVQueryRequest_MOD:
	case flockpb.KVQueryRequest_VALUE:
		// full scan always
		// after cursor depends on sort target
	}

	return nil
}

func After(sortTarget flockpb.KVQueryRequest_SortTarget, sortOrder flockpb.KVQueryRequest_SortOrder, after string) stream.Processor {
	switch sortTarget {
	case flockpb.KVQueryRequest_KEY:
		// No after required since the natural ordering of the keys takes care of this
		return nil
	case flockpb.KVQueryRequest_VERSION:
		return stream.Filter(func(a interface{}) bool {
			if len(a.([]byte)) != 8 {
				return true
			}

			v := int64(binary.BigEndian.Uint64(a.([]byte)[:]))

			return a.(mvcc.UnmarshaledKV)[1].(flockpb.KeyValue).Version > v
		})
	case flockpb.KVQueryRequest_CREATE:
		return stream.Filter(func(a interface{}) bool {
			if len(a.([]byte)) != 8 {
				return true
			}

			v := int64(binary.BigEndian.Uint64(a.([]byte)[:]))

			return a.(mvcc.UnmarshaledKV)[1].(flockpb.KeyValue).CreateRevision > v
		})
	case flockpb.KVQueryRequest_MOD:
		return stream.Filter(func(a interface{}) bool {
			if len(a.([]byte)) != 8 {
				return true
			}

			v := int64(binary.BigEndian.Uint64(a.([]byte)[:]))

			return a.(mvcc.UnmarshaledKV)[1].(flockpb.KeyValue).ModRevision > v
		})
	case flockpb.KVQueryRequest_VALUE:
		return stream.Filter(func(a interface{}) bool {
			return bytes.Compare(a.(mvcc.UnmarshaledKV)[1].(flockpb.KeyValue).Value, []byte(after)) > 0
		})
	default:
		return nil
	}
}

func afterVersion(sortTarget flockpb.KVQueryRequest_SortTarget, sortOrder flockpb.KVQueryRequest_SortOrder, after string) func(a interface{}) bool {
	return nil
}

func afterCreate(sortTarget flockpb.KVQueryRequest_SortTarget, sortOrder flockpb.KVQueryRequest_SortOrder, after string) func(a interface{}) bool {
	return nil
}

func afterMod(sortTarget flockpb.KVQueryRequest_SortTarget, sortOrder flockpb.KVQueryRequest_SortOrder, after string) func(a interface{}) bool {
	return nil
}

func afterValue(sortTarget flockpb.KVQueryRequest_SortTarget, sortOrder flockpb.KVQueryRequest_SortOrder, after string) func(a interface{}) bool {
	return nil
}
