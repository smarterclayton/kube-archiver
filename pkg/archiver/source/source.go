package source

import (
	"encoding/json"
)

type uidMetadata struct {
	UID string `json:"uid"`
}

type uidObject struct {
	Metadata uidMetadata `json:"metadata"`
}

func ExtractUID(value []byte) (string, bool) {
	obj := uidObject{}
	if err := json.Unmarshal(value, &obj); err != nil {
		return "", false
	}
	if len(obj.Metadata.UID) > 0 {
		return obj.Metadata.UID, true
	}
	return "", false
}
