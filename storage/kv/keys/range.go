package keys

import (
	"bytes"
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
	Min []byte
	Max []byte
	ns  []byte
}

// Eq confines the range to just key k
func (r Range) Eq(k []byte) Range {
	return r.Gte(k).Lte(k)
}

// Gt confines the range to keys that are
// greater than k
func (r *Range) Gt(k []byte) Range {
	return r.refineMin(after(k))
}

// Gte confines the range to keys that are
// greater than or equal to k
func (r Range) Gte(k []byte) Range {
	return r.refineMin(k)
}

// Lt confines the range to keys that are
// less than k
func (r Range) Lt(k []byte) Range {
	return r.refineMax(k)
}

// Lte confines the range to keys that are
// less than or equal to k
func (r Range) Lte(k []byte) Range {
	return r.refineMax(after(k))
}

// Prefix confines the range to keys that
// have the prefix k, excluding k itself
func (r Range) Prefix(k []byte) Range {
	return r.Gt(k).Lt(inc(k))
}

// Namespace namespaces keys in the range with
// to keys with the prefix ns. Subsequent modifier
// methods will keep keys within this namespace.
func (r Range) Namespace(ns []byte) Range {
	r.Min = prefix(r.Min, ns)
	r.Max = inc(prefix(r.Max, ns))
	r.ns = append(r.ns, ns...)

	return r
}

func (r Range) refineMin(min []byte) Range {
	if len(r.ns) > 0 {
		min = prefix(min, r.ns)
	}

	if compare(min, r.Min) <= 0 {
		return r
	}

	r.Min = min

	return r
}

func (r Range) refineMax(max []byte) Range {
	if len(r.ns) > 0 {
		max = inc(prefix(max, r.ns))
	}

	if r.Max != nil && compare(max, r.Max) >= 0 {
		return r
	}

	r.Max = max

	return r
}

func compare(a []byte, b []byte) int {
	if a == nil {
		if b == nil {
			return 0
		}

		return -1
	}

	if b == nil {
		return 1
	}

	return bytes.Compare(a, b)
}

// after returns the key directly after k such that
// there can exist no other key that comes between
// k and after(k)
func after(k []byte) []byte {
	afterK := make([]byte, len(k)+1)

	copy(afterK, k)
	afterK[len(k)] = 0

	return afterK
}

// inc treats k as a big-endian unsigned integer
// and adds 1 to it
func inc(k []byte) []byte {
	carry := true

	for i := len(k) - 1; i >= 0 && carry; i-- {
		if k[i] < 0xff {
			carry = false
		}

		k[i]++
	}

	// carry will only be true if all elements of k
	// were equal to 0xff. The range should just go
	// all the way to the end of the real key range.
	if carry {
		return nil
	}

	return k
}

// prefix appends k to p
func prefix(k []byte, p []byte) []byte {
	if len(k) == 0 && len(p) == 0 {
		return k
	}

	prefixedK := make([]byte, 0, len(p)+len(k))
	prefixedK = append(prefixedK, p...)
	prefixedK = append(prefixedK, k...)

	return prefixedK
}
