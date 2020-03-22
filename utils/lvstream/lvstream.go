package lvstream

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"sync"
)

// 10 MB
var MaxValueSize = 10 * 1024 * 1024 * 1024

// msg,msg,msg -> [length|msg|length|msg...]
// [length|msg|length|msg...] -> msg,msg,msg
var EClosed = errors.New("Closed")

var _ io.ReadCloser = (*LVStreamEncoder)(nil)

type LVStreamEncoder struct {
	nextValue func() ([]byte, error)
	cleanup   func()
	isLength  bool
	length    []byte
	value     []byte
	chunk     []byte
	err       error
}

func NewLVStreamEncoder(nextValue func() ([]byte, error), cleanup func()) *LVStreamEncoder {
	encoder := &LVStreamEncoder{
		length:    make([]byte, 4),
		nextValue: nextValue,
		cleanup:   cleanup,
	}

	return encoder
}

// Read implements io.Reader
// It will panic if any of the preconditions
// are not met.
func (encoder *LVStreamEncoder) Read(p []byte) (int, error) {
	if encoder.err != nil {
		return 0, encoder.err
	}

	n := 0
	pLen := len(p)

	for n < pLen {
		if len(encoder.chunk) == 0 {
			if encoder.isLength {
				encoder.isLength = false
				encoder.chunk = encoder.value
			} else {
				encoder.isLength = true
				value, err := encoder.nextValue()

				if err != nil {
					encoder.close(err)

					return n, encoder.err
				}

				encoder.value = value
				binary.BigEndian.PutUint32(encoder.length, uint32(len(value)))
				encoder.chunk = encoder.length
			}
		}

		c := copy(p, encoder.chunk)
		encoder.chunk = encoder.chunk[c:]
		p = p[c:]
		n += c
	}

	return n, nil
}

func (encoder *LVStreamEncoder) close(err error) {
	if encoder.err != nil {
		return
	}

	encoder.err = err
	encoder.cleanup()
}

func (encoder *LVStreamEncoder) Close() error {
	encoder.close(EClosed)

	return nil
}

var _ io.WriteCloser = (*LVStreamDecoder)(nil)

type LVStreamDecoder struct {
	nextValue func([]byte) error
	isLength  bool
	chunkSize int
	chunk     []byte
	errMu     sync.Mutex
	err       error
}

func NewLVStreamDecoder(nextValue func([]byte) error) *LVStreamDecoder {
	decoder := &LVStreamDecoder{
		chunkSize: 4,
		isLength:  true,
		nextValue: nextValue,
	}

	decoder.chunk = reallocate(decoder.chunk, decoder.chunkSize)

	return decoder
}

func (decoder *LVStreamDecoder) Write(p []byte) (int, error) {
	if decoder.err != nil {
		return 0, decoder.err
	}

	pLen := len(p)

	for len(p) > 0 {
		// cap(chunk) >= chunkSize
		// copy p to chunk up to min(len(p), chunkSize)
		copyAmount := min(decoder.chunkSize, len(p))
		decoder.chunk = append(decoder.chunk, p[:copyAmount]...)
		p = p[copyAmount:]

		// Have we read all bytes for the current chunk?
		if len(decoder.chunk) == decoder.chunkSize {
			if decoder.isLength {
				// It's the length prefix. This becomes our new chunk size
				length := binary.BigEndian.Uint32(decoder.chunk)

				if length > uint32(MaxValueSize) {
					decoder.err = fmt.Errorf("Encoded value length is too large: %d > max(%d)", length, MaxValueSize)

					return 0, decoder.err
				}

				decoder.chunkSize = int(length)
				decoder.chunk = reallocate(decoder.chunk, decoder.chunkSize)
				decoder.isLength = false
			} else {
				// It's the next value. Call submit
				if err := decoder.nextValue(decoder.chunk); err != nil {
					decoder.err = err

					return 0, decoder.err
				}

				decoder.chunkSize = 4
				decoder.chunk = reallocate(decoder.chunk, decoder.chunkSize)
				decoder.isLength = true
			}
		}
	}

	return pLen, nil
}

func (decoder *LVStreamDecoder) close(err error) error {
	decoder.errMu.Lock()
	defer decoder.errMu.Unlock()

	if decoder.err == nil {
		decoder.err = err
	}

	return decoder.err
}

func (decoder *LVStreamDecoder) Close() error {
	return decoder.close(EClosed)
}

func min(a, b int) int {
	if a > b {
		return b
	}

	return a
}

func reallocate(b []byte, capacity int) []byte {
	if cap(b) < capacity {
		return make([]byte, 0, capacity)
	}

	return b[:0]
}
