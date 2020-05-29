package stream_test

import (
	"math/rand"
	"sort"
	"testing"

	"github.com/google/go-cmp/cmp"
	"github.com/jrife/flock/utils/stream"
)

func ints(n int) stream.Stream {
	return &randomIntStream{n, 0}
}

type randomIntStream struct {
	n int
	v int
}

func (stream *randomIntStream) Next() bool {
	if stream.n > 0 {
		stream.n--
		stream.v = rand.Int()

		return true
	}

	return false
}

func (stream *randomIntStream) Value() interface{} {
	return stream.v
}

func (stream *randomIntStream) Error() error {
	return nil
}

func record(record *[]int) stream.Processor {
	*record = []int{}

	return func(stream stream.Stream) stream.Stream {
		return &streamRecorder{stream, record}
	}
}

type streamRecorder struct {
	stream.Stream
	record *[]int
}

func (stream *streamRecorder) Next() bool {
	if !stream.Stream.Next() {
		return false
	}

	*stream.record = append(*stream.record, stream.Value().(int))

	return true
}

func Drain(stream stream.Stream) {
	for stream.Next() {
	}
}

func Filter(ints []int, filter func(a interface{}) bool) []int {
	filteredInts := []int{}

	for _, i := range ints {
		if filter(i) {
			filteredInts = append(filteredInts, i)
		}
	}

	return filteredInts
}

func Sort(ints []int) []int {
	sort.Ints(ints)

	return ints
}

func Limit(ints []int, limit int) []int {
	if limit <= 0 || limit > len(ints) {
		return ints
	}

	return ints[:limit]
}

func Reverse(ints []int) []int {
	reversed := []int{}

	for i := len(ints) - 1; i >= 0; i-- {
		reversed = append(reversed, ints[i])
	}

	return reversed
}

func Negate(compare func(a, b interface{}) int) func(a, b interface{}) int {
	return func(a, b interface{}) int {
		return -1 * compare(a, b)
	}
}

func compare(a, b interface{}) int {
	if a.(int) < b.(int) {
		return -1
	} else if a.(int) > b.(int) {
		return 1
	}

	return 0
}

func TestStream(t *testing.T) {
	positive := func(a interface{}) bool { return a.(int) > 0 }
	limit := 10

	input := []int{}
	output := []int{}

	Drain(stream.Pipeline(ints(1000), record(&input), stream.Filter(positive), stream.Sort(compare, -1), stream.Limit(limit), record(&output)))
	diff := cmp.Diff(Limit(Sort(Filter(input, positive)), limit), output)

	if diff != "" {
		t.Fatalf(diff)
	}

	Drain(stream.Pipeline(ints(1000), record(&input), stream.Filter(positive), stream.Sort(compare, -1), record(&output)))
	diff = cmp.Diff(Sort(Filter(input, positive)), output)

	if diff != "" {
		t.Fatalf(diff)
	}

	Drain(stream.Pipeline(ints(1000), record(&input), stream.Filter(positive), stream.Sort(Negate(compare), -1), record(&output)))
	diff = cmp.Diff(Reverse(Sort(Filter(input, positive))), output)

	if diff != "" {
		t.Fatalf(diff)
	}

	Drain(stream.Pipeline(ints(1000), record(&input), stream.Filter(positive), stream.Sort(Negate(compare), -1), stream.Limit(limit), record(&output)))
	diff = cmp.Diff(Limit(Reverse(Sort(Filter(input, positive))), limit), output)

	if diff != "" {
		t.Fatalf(diff)
	}
}
