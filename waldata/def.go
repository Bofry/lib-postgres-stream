package waldata

import "encoding/json"

const (
	TAG_KEY = "key"
)

func ExtractWal2JsonData(buf []byte) (Wal2JsonDataSet, error) {
	var data Wal2JsonDataSet

	err := json.Unmarshal(buf, &data)
	if err != nil {
		return nil, err
	}
	return data, nil
}
