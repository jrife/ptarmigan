package sequence

// Element is
type Element interface {
	Next() Element
	Compare(e Element) int
}

type Interface interface {
	Len() int
	Get(i int) Element
	Set(i int, e Element)
	Inc() Interface
	Copy() Interface
	AppendMinElement() Interface
}

func Next(sequence Interface) Interface {
	return sequence.AppendMinElement()
}

func Compare(a, b Interface) int {
	if a == nil && b != nil {
		return -1
	} else if a != nil && b == nil {
		return 1
	}

	var i int
	var cmp int

	for ; i < a.Len() && i < b.Len() && cmp == 0; cmp, i = a.Get(i).Compare(b.Get(i)), i+1 {
	}

	if cmp != 0 {
		return cmp
	}

	if a.Len() < b.Len() {
		return -1
	} else if a.Len() > b.Len() {
		return 1
	}

	return 0
}

func Inc(sequence Interface) Interface {
	carry := true
	after := sequence.Copy()

	for i := after.Len() - 1; i >= 0 && carry; i-- {
		n := after.Get(i).Next()

		if n == nil {
			continue
		}

		after.Set(i, n)
		carry = false
	}

	// carry will only be true if all elements of k
	// were equal to 0xff. The range should just go
	// all the way to the end of the real key range.
	if carry {
		return nil
	}

	return after
}
