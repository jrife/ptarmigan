package storage

import (
	"fmt"

	"github.com/jrife/flock/storage/mvcc"
)

func wrapError(wrap string, err error) error {
	switch err {
	case mvcc.ErrCompacted:
		fallthrough
	case mvcc.ErrNoRevisions:
		fallthrough
	case mvcc.ErrRevisionTooHigh:
		fallthrough
	case nil:
		return err
	}

	return fmt.Errorf("%s: %s", wrap, err)
}
