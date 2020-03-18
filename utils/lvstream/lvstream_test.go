package lvstream_test

import (
	"io"
	"reflect"
	"testing"

	"github.com/jrife/ptarmigan/utils/lvstream"
)

func TestLVStreamReader(t *testing.T) {

}

func TestLVStreamWriter(t *testing.T) {

}

func TestLVStream(t *testing.T) {
	input := [][]byte{
		[]byte("a"),
		[]byte("b"),
		[]byte("c"),
	}
	output := [][]byte{}

	lvReader := &lvstream.LVStreamReader{
		ReadValue: func() ([]byte, error) {
			if len(input) == 0 {
				return nil, io.EOF
			}

			next := input[0]
			input = input[1:]

			return next, nil
		},
	}
	lvWriter := &lvstream.LVStreamWriter{
		WriteValue: func(value []byte) error {
			output = append(output, value)

			return nil
		},
	}

	io.Copy(lvWriter, lvReader)

	if !reflect.DeepEqual(input, output) {
		t.Errorf("%v != %v", input, output)
	}
}
