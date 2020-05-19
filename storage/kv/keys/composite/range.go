package composite

import (
	"github.com/jrife/ptarmigan/utils/sequence"
)

// All returns a new key range matching all keys
func All() Range {
	return Range{}
}

// Range represents all keys such that
//   k >= Min and k < Max
// If Min = nil that indicates the start of all keys
// If Max = nil that indicatese the end of all keys
// If multiple modifiers are called on a range the end
// result is effectively the same as ANDing all the
// restrictions.
type Range struct {
	Min Key
	Max Key
	ns  Key
}

// Eq confines the range to just key k
func (r Range) Eq(k Key) Range {
	return r.Gte(k).Lte(k)
}

// Gt confines the range to keys that are
// greater than k
func (r Range) Gt(k Key) Range {
	return r.refineMin(sequence.Next(k).(Key))
}

// Gte confines the range to keys that are
// greater than or equal to k
func (r Range) Gte(k Key) Range {
	return r.refineMin(k)
}

// Lt confines the range to keys that are
// less than k
func (r Range) Lt(k Key) Range {
	return r.refineMax(k)
}

// Lte confines the range to keys that are
// less than or equal to k
func (r Range) Lte(k Key) Range {
	return r.refineMax(sequence.Next(k).(Key))
}

// Prefix confines the range to keys that
// have the prefix k, excluding k itself
func (r Range) Prefix(k Key) Range {
	return r.Gt(k).Lt(sequence.Inc(k).(Key))
}

// Namespace namespaces keys in the range with
// to keys with the prefix ns. Subsequent modifier
// methods will keep keys within this namespace.
func (r Range) Namespace(ns Key) Range {
	r.Min = prefix(r.Min, ns)

	if r.Max == nil {
		r.Max = sequence.Inc(ns).(Key)
	} else {
		r.Max = prefix(r.Max, ns)
	}

	r.ns = append(r.ns, ns...)

	return r
}

func (r Range) refineMin(min Key) Range {
	if len(r.ns) > 0 {
		min = prefix(min, r.ns)
	}

	if sequence.Compare(min, r.Min) <= 0 {
		return r
	}

	r.Min = min

	return r
}

func (r Range) refineMax(max Key) Range {
	if len(r.ns) > 0 {
		if max == nil {
			max = sequence.Inc(r.ns).(Key)
		} else {
			max = prefix(max, r.ns)
		}
	}

	if r.Max != nil && sequence.Compare(max, r.Max) >= 0 {
		return r
	}

	r.Max = max

	return r
}

// prefix appends k to p
func prefix(k Key, p Key) Key {
	if len(k) == 0 && len(p) == 0 {
		return k
	}

	prefixedK := make(Key, 0, len(p)+len(k))
	prefixedK = append(prefixedK, p...)
	prefixedK = append(prefixedK, k...)

	return prefixedK
}
