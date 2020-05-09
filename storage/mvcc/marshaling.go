package mvcc

import (
	"fmt"
)

// UnmarshaledKV is a generic tuple for any key-value types
type UnmarshaledKV [2]interface{}

// UnmarshaledDiff is a generic tuple for any key-value diff
type UnmarshaledDiff [3]interface{}

// Marshalable describes a type that can created a marshaled
// representation
type Marshalable interface {
	Marshal() ([]byte, error)
}

// Unmarshaler unmarshals a byte slice into a concrete
// type
type Unmarshaler func(k []byte) (interface{}, error)

// NewMarshalingView wraps a View such that keys and values are marshaled and unmarshaled automatically
func NewMarshalingView(view View, keyUnmarshaler Unmarshaler, valueUnmarshaler Unmarshaler) *MarshalingView {
	return &MarshalingView{
		view:           view,
		unmarshalKey:   keyUnmarshaler,
		unmarshalValue: valueUnmarshaler,
	}
}

// NewMarshalingRevision wraps a Revision such that keys and values are marshaled and unmarshaled automatically
func NewMarshalingRevision(revision Revision, keyUnmarshaler Unmarshaler, valueUnmarshaler Unmarshaler) *MarshalingRevision {
	return &MarshalingRevision{
		MarshalingView: MarshalingView{
			view:           revision,
			unmarshalKey:   keyUnmarshaler,
			unmarshalValue: valueUnmarshaler,
		},
	}
}

// MarshalingRevision wraps a Revision such that keys and values are automatically marshaled and unmarshaled
type MarshalingRevision struct {
	MarshalingView
}

// Put is like Revision.Put, but it marshals the key and value.
func (revision *MarshalingRevision) Put(key Marshalable, value Marshalable) error {
	marshaledKey, err := key.Marshal()

	if err != nil {
		return fmt.Errorf("could not marshal key: %s", err)
	}

	marshaledValue, err := value.Marshal()

	if err != nil {
		return fmt.Errorf("could not marshal value: %s", err)
	}

	return revision.view.(Revision).Put(marshaledKey, marshaledValue)
}

// Delete is like Revision.Delete, but it marshals the key
func (revision *MarshalingRevision) Delete(key Marshalable) error {
	marshaledKey, err := key.Marshal()

	if err != nil {
		return fmt.Errorf("could not marshal key: %s", err)
	}

	return revision.view.(Revision).Delete(marshaledKey)
}

// MarshalingView wraps a View such that keys and values are automatically marshaled and unmarshaled
type MarshalingView struct {
	view           View
	unmarshalKey   Unmarshaler
	unmarshalValue Unmarshaler
}

func (view *MarshalingView) unmarshal(key []byte, value []byte) (interface{}, interface{}, error) {
	k, err := view.unmarshalKey(key)

	if err != nil {
		return nil, nil, fmt.Errorf("could not unmarshal key %#v: %s", key, err)
	}

	v, err := view.unmarshalValue(value)

	if err != nil {
		return nil, nil, fmt.Errorf("could not unmarshal value %#v: %s", value, err)
	}

	return k, v, nil
}

func (view *MarshalingView) unmarshalDiff(key []byte, currentValue []byte, prevValue []byte) (interface{}, interface{}, interface{}, error) {
	k, err := view.unmarshalKey(key)

	if err != nil {
		return nil, nil, nil, fmt.Errorf("could not unmarshal key %#v: %s", key, err)
	}

	var current interface{}
	var prev interface{}

	if currentValue != nil {
		current, err = view.unmarshalValue(currentValue)

		if err != nil {
			return nil, nil, nil, fmt.Errorf("could not unmarshal value %#v: %s", currentValue, err)
		}
	}

	if prevValue != nil {
		prev, err = view.unmarshalValue(prevValue)

		if err != nil {
			return nil, nil, nil, fmt.Errorf("could not unmarshal value %#v: %s", prevValue, err)
		}
	}

	return k, current, prev, nil
}

func (view *MarshalingView) marshalKey(k Marshalable) ([]byte, error) {
	if k == nil {
		return nil, nil
	}

	return k.Marshal()
}

// Keys is like View.Keys, but it marshals the min and max parameters and returns unmarshaled key-value pairs.
func (view *MarshalingView) Keys(min Marshalable, max Marshalable, limit int, sort SortOrder) ([]UnmarshaledKV, error) {
	marshaledMin, err := view.marshalKey(min)

	if err != nil {
		return nil, fmt.Errorf("could not marshal min: %s", err)
	}

	marshaledMax, err := view.marshalKey(max)

	if err != nil {
		return nil, fmt.Errorf("could not marshal max: %s", err)
	}

	kvs, err := view.view.Keys(marshaledMin, marshaledMax, limit, sort)

	if err != nil {
		return nil, err
	}

	unmarshaledKVs := make([]UnmarshaledKV, len(kvs))

	for i, kv := range kvs {
		k, v, err := view.unmarshal(kv[0], kv[1])

		if err != nil {
			return nil, fmt.Errorf("could not unmarshal kv %#v: %s", kv, err)
		}

		unmarshaledKVs[i] = UnmarshaledKV{k, v}
	}

	return unmarshaledKVs, nil
}

// KeysIterator is like View.KeysIterator, but it marshals the min and max parameters and its iterator returns unmarshaled key-value pairs.
func (view *MarshalingView) KeysIterator(min Marshalable, max Marshalable, order SortOrder) (UnmarshaledIterator, error) {
	marshaledMin, err := view.marshalKey(min)

	if err != nil {
		return nil, fmt.Errorf("could not marshal min: %s", err)
	}

	marshaledMax, err := view.marshalKey(max)

	if err != nil {
		return nil, fmt.Errorf("could not marshal max: %s", err)
	}

	iter, err := view.view.KeysIterator(marshaledMin, marshaledMax, order)

	if err != nil {
		return nil, fmt.Errorf("could not create keys iterator: %s", err)
	}

	return &unmarshaledIterator{Iterator: iter, view: view}, nil
}

// Changes is like View.Changes, but it marshals the min and max parameters and it returns unmarshaled diffs.
func (view *MarshalingView) Changes(min Marshalable, max Marshalable, limit int, includePrev bool) ([]UnmarshaledDiff, error) {
	marshaledMin, err := view.marshalKey(min)

	if err != nil {
		return nil, fmt.Errorf("could not marshal min: %s", err)
	}

	marshaledMax, err := view.marshalKey(max)

	if err != nil {
		return nil, fmt.Errorf("could not marshal max: %s", err)
	}

	diffs, err := view.view.Changes(marshaledMin, marshaledMax, limit, includePrev)

	if err != nil {
		return nil, err
	}

	unmarshaledDiffs := make([]UnmarshaledDiff, len(diffs))

	for i, diff := range diffs {
		k, c, p, err := view.unmarshalDiff(diff[0], diff[1], diff[2])

		if err != nil {
			return nil, fmt.Errorf("could not unmarshal diff %#v: %s", diff, err)
		}

		unmarshaledDiffs[i] = UnmarshaledDiff{k, c, p}
	}

	return unmarshaledDiffs, nil
}

// Revision returns this view's revision number.
func (view *MarshalingView) Revision() int64 {
	return view.view.Revision()
}

// Close closes this view
func (view *MarshalingView) Close() error {
	return view.view.Close()
}

// UnmarshaledIterator is like Iterator but it unmarshals keys and values.
type UnmarshaledIterator interface {
	Next() bool
	Key() interface{}
	Value() interface{}
	Error() error
}

var _ UnmarshaledIterator = (*unmarshaledIterator)(nil)

type unmarshaledIterator struct {
	Iterator
	view  *MarshalingView
	key   interface{}
	value interface{}
	err   error
}

func (iter *unmarshaledIterator) Next() bool {
	if iter.err != nil {
		return false
	}

	if !iter.Iterator.Next() {
		return false
	}

	iter.key, iter.value, iter.err = iter.view.unmarshal(iter.Iterator.Key(), iter.Iterator.Value())

	if iter.err != nil {
		return false
	}

	return true
}

func (iter *unmarshaledIterator) Key() interface{} {
	return iter.key
}

func (iter *unmarshaledIterator) Value() interface{} {
	return iter.value
}

func (iter *unmarshaledIterator) Error() error {
	if iter.err != nil {
		return iter.err
	}

	return iter.Iterator.Error()
}
