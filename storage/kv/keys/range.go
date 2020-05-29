package keys

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
func (r Range) Gt(k []byte) Range {
	return r.refineMin(Next(k))
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
	return r.refineMax(Next(k))
}

// Prefix confines the range to keys that
// have the prefix k, excluding k itself
func (r Range) Prefix(k []byte) Range {
	return r.Gt(k).Lt(Inc(k))
}

// Namespace namespaces keys in the range with
// to keys with the prefix ns. Subsequent modifier
// methods will keep keys within this namespace.
func (r Range) Namespace(ns []byte) Range {
	r.Min = prefix(r.Min, ns)

	if r.Max == nil {
		r.Max = Inc(ns)

	} else {
		r.Max = prefix(r.Max, ns)
	}

	r.ns = append(r.ns, ns...)

	return r
}

// Contains returns true if the range contains
// key
func (r Range) Contains(key Key) bool {
	if r.Min != nil && Compare(key, r.Min) < 0 {
		return false
	}

	if r.Max != nil && Compare(key, r.Max) >= 0 {
		return false
	}

	return true
}

func (r Range) refineMin(min []byte) Range {
	if len(r.ns) > 0 {
		min = prefix(min, r.ns)
	}

	if Compare(min, r.Min) <= 0 {
		return r
	}

	r.Min = min

	return r
}

func (r Range) refineMax(max []byte) Range {
	if len(r.ns) > 0 {
		if max == nil {
			max = Inc(r.ns)
		} else {
			max = prefix(max, r.ns)
		}
	}

	if r.Max != nil && Compare(max, r.Max) >= 0 {
		return r
	}

	r.Max = max

	return r
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
