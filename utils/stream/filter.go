package stream

// Filter filters out elements from the source stream for
// which the filter function retruns false
func Filter(filter func(value interface{}) bool) Processor {
	return func(stream Stream) Stream {
		return &filteredStream{stream, filter}
	}
}

type filteredStream struct {
	Stream
	filter func(value interface{}) bool
}

func (stream *filteredStream) Next() bool {
	hasMore := false

	for hasMore = stream.Stream.Next(); hasMore && !stream.filter(stream.Value()); hasMore = stream.Stream.Next() {
	}

	return hasMore
}
