package waldata

import "encoding/json"

const (
	TAG_KEY = "key"

	KIND_DELETE  = "delete"
	KIND_INSERT  = "insert"
	KIND_UPDATE  = "update"
	KIND_MESSAGE = "message"
)

func ExtractWal2JsonData(buf []byte) (Wal2JsonDataSet, error) {
	var data Wal2JsonDataSet

	err := json.Unmarshal(buf, &data)
	if err != nil {
		return nil, err
	}
	return data, nil
}
