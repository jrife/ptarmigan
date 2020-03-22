package uuid

import (
	google_uuid "github.com/google/uuid"
)

func MustUUID() string {
	return google_uuid.New().String()
}
