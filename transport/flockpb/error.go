package flockpb

import (
	"fmt"
)

func (err *Error) Error() string {
	return fmt.Sprintf("%s: %s: %s", err.Code.String(), err.Type, err.Message)
}
