package stream

import "github.com/jrife/ptarmigan/utils/sortedwindow"

// Sort finds the lowest N elements in a stream as defined by the comparison
// function and returns them in ascending order. If limit > 0 then N = limit,
// otherwise N = the size of the stream. In other words, if limit is <= 0 then
// it sorts the entire collection.
func Sort(compare func(a interface{}, b interface{}) int, limit int) Processor {
	return func(stream Stream) Stream {
		return &sortedStream{
			stream,
			sortedwindow.New(compare, sortedwindow.WithLimit(limit)),
			nil,
		}
	}
}

type sortedStream struct {
	Stream
	window *sortedwindow.SortedMinWindow
	iter   *sortedwindow.Iterator
}

func (stream *sortedStream) Next() bool {
	if stream.iter == nil {
		for stream.Stream.Next() {
			stream.window.Insert(stream.Stream.Value())
		}

		if stream.Stream.Error() != nil {
			return false
		}

		stream.iter = stream.window.Iterator()
	}

	return stream.iter.Next()
}

func (stream *sortedStream) Value() interface{} {
	return stream.iter.Value()
}
