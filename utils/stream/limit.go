package stream

// Limit limits the number of streamed elements
// If limit <= 0 then there is no limit, it will
// return all elements from the source stream.
// Otherwise it will only return up to the first limit
// elements.
func Limit(limit int) Processor {
	return func(stream Stream) Stream {
		return &limitedStream{stream, limit}
	}
}

type limitedStream struct {
	Stream
	remaining int
}

func (stream *limitedStream) Next() bool {
	if stream.remaining <= 0 {
		return false
	}

	stream.remaining--

	return stream.Stream.Next()
}
