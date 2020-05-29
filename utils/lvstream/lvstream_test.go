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
	values := [][]byte{
		[]byte("a"),
		[]byte("b"),
		[]byte("c"),
	}
	input := values
	output := [][]byte{}

	lvReader := lvstream.NewLVStreamEncoder(func() ([]byte, error) {
		if len(input) == 0 {
			return nil, io.EOF
		}

		next := input[0]
		input = input[1:]

		return next, nil
	}, func() {})

	lvWriter := lvstream.NewLVStreamDecoder(func(value []byte) error {
		valueCopy := append([]byte{}, value...)
		output = append(output, valueCopy)

		return nil
	})

	if _, err := io.Copy(lvWriter, lvReader); err != nil {
		t.Fatalf("io.Copy error: %s", err.Error())
	}

	if !reflect.DeepEqual(values, output) {
		t.Errorf("%v != %v", values, output)
	}
}
