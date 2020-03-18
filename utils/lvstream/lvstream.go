package lvstream

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
)

// 10 MB
var MaxValueSize = 10 * 1024 * 1024 * 1024

// msg,msg,msg -> [length|msg|length|msg...]
// [length|msg|length|msg...] -> msg,msg,msg
var EClosed = errors.New("Closed")

var _ io.ReadCloser = (*LVStreamReader)(nil)

type LVStreamReader struct {
	ReadValue func() ([]byte, error)
	isLength  bool
	length    []byte
	value     []byte
	chunk     []byte
	err       error
}

// Read implements io.Reader
// It will panic if any of the preconditions
// are not met.
// Preconditions:
// - ReadValue is not nil
// - ReadValue must not return a nil value
// - ReadValue must not return an empty value
// - ReadValue must not return a value whose length is greater than math.MaxUint32
func (reader *LVStreamReader) Read(p []byte) (int, error) {
	if reader.err != nil {
		return 0, reader.err
	}

	// Precondition check. Programmer error
	if reader.ReadValue == nil {
		panic("ReadValue cannot be nil")
	}

	// lazy init
	if reader.chunk == nil {
		reader.isLength = false
		reader.length = make([]byte, 4)
		reader.chunk = reader.value
	}

	n := 0

	for n < len(p) {
		if len(reader.chunk) == 0 {
			if reader.isLength {
				reader.isLength = false
				reader.chunk = reader.value
			} else {
				reader.isLength = true
				value, err := reader.readValue()

				if err != nil {
					reader.err = err

					return n, reader.err
				}

				reader.value = value
				binary.BigEndian.PutUint32(reader.length, uint32(len(value)))
				reader.chunk = reader.length
			}
		}

		c := copy(p, reader.chunk)
		reader.chunk = reader.chunk[c:]
		p = p[c:]
		n += c
	}

	return n, nil
}

func (reader *LVStreamReader) readValue() ([]byte, error) {
	value, err := reader.ReadValue()

	if err != nil {
		return nil, err
	}

	// Precondition check. Programmer error
	if len(value) == 0 {
		panic("Encoded value size is 0")
	} else if len(value) > MaxValueSize {
		panic(fmt.Sprintf("Encoded value size is too large %d > max(%d)", len(value), MaxValueSize))
	}

	return value, nil
}

func (reader *LVStreamReader) Close() error {
	if reader.err != nil {
		return nil
	}

	reader.err = EClosed

	return nil
}

var _ io.WriteCloser = (*LVStreamWriter)(nil)

type LVStreamWriter struct {
	WriteValue func(value []byte) error
	isLength   bool
	chunkSize  int
	chunk      []byte
	err        error
}

func (writer *LVStreamWriter) Write(p []byte) (int, error) {
	if writer.err != nil {
		return 0, writer.err
	}

	// lazy init
	if writer.chunk == nil {
		writer.chunkSize = 4
		writer.chunk = reallocate(writer.chunk, writer.chunkSize)
		writer.isLength = true
	}

	for len(p) > 0 {
		// Have we read all bytes for the current chunk?
		if len(writer.chunk) == writer.chunkSize {
			if writer.isLength {
				// It's the length prefix. This becomes our new chunk size
				length := binary.BigEndian.Uint32(writer.chunk)

				if length > uint32(MaxValueSize) {
					writer.err = fmt.Errorf("Encoded value length is too large: %d > max(%d)", length, MaxValueSize)

					return 0, writer.err
				}

				writer.chunkSize = int(length)
				writer.chunk = reallocate(writer.chunk, writer.chunkSize)
				writer.isLength = false
			} else {
				// Precondition check. Programmer error
				if writer.WriteValue == nil {
					panic("WriteValue cannot be nil")
				}

				// It's the next value. Call WriteValue
				if err := writer.WriteValue(writer.chunk); err != nil {
					writer.err = err

					return 0, writer.err
				}

				writer.chunkSize = 4
				writer.chunk = reallocate(writer.chunk, writer.chunkSize)
				writer.isLength = true
			}
		}

		// cap(chunk) >= chunkSize
		// copy p to chunk up to max(len(p), chunkSize)
		copyAmount := max(writer.chunkSize, len(p))
		writer.chunk = append(writer.chunk, p[:copyAmount]...)
		p = p[copyAmount:]
	}

	return len(p), nil
}

func max(a, b int) int {
	if a < b {
		return b
	}

	return a
}

func (writer *LVStreamWriter) Close() error {
	if writer.err != nil {
		return nil
	}

	writer.err = EClosed

	return nil
}

func reallocate(b []byte, capacity int) []byte {
	if cap(b) < capacity {
		return make([]byte, 0, capacity)
	}

	return b[:0]
}
