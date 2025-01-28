package waldata

import "encoding/json"

var (
	_ json.Unmarshaler = new(Wal2JsonDataSet)
)

type Wal2JsonDataSet []Wal2JsonData

// UnmarshalJSON implements json.Unmarshaler.
func (w *Wal2JsonDataSet) UnmarshalJSON(data []byte) error {
	type Alias Wal2JsonDataSet
	v := &struct {
		Change []Wal2JsonData `json:"change"`
		*Alias
	}{
		Alias: (*Alias)(w),
	}

	if err := json.Unmarshal(data, &v); err != nil {
		return err
	}

	*w = v.Change
	return nil
}
