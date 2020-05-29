package stream

// Stream describes a stream of values
type Stream interface {
	// Next advances the stream. It must
	// be called once at the start to advance
	// to the first item in the stream. It returns
	// true if there is a value available
	// or false otherwise. It may return false in
	// case of an error. Error() will return
	// an error if this is the case and must be checked
	// after Next() returns false.
	Next() bool
	// Value returns the value at the current position
	// or nil if iteration is done.
	Value() interface{}
	// Error returns the error that occurred, if any
	Error() error
}

// Processor is a function that returns a stream
// derived from a source stream.
type Processor func(Stream) Stream

// Pipeline connects a series of processors to a source
// stream and returns the derived stream. Pipeline can
// be useful to create code that is more readable than
// simply invoking processor functions in a nested way
// like this processor3(processor2(processor1(stream)))
func Pipeline(stream Stream, processors ...Processor) Stream {
	for _, processor := range processors {
		if processor == nil {
			continue
		}

		stream = processor(stream)
	}

	return stream
}
