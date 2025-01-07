package postgres

import (
	"encoding/json"
)

var (
	_ json.Unmarshaler = new(CreateReplicationSlotSource)
)

type CreateReplicationSlotSource struct {
	SlotName       string          `json:"SlotName"`
	Plugin         string          `json:"Plugin"`
	Temporary      bool            `json:"Temporary"`
	SlotType       ReplicationMode `json:"SlotType"`
	SnapshotAction string          `json:"SnapshotAction"`
}

// UnmarshalJSON implements json.Unmarshaler.
func (c *CreateReplicationSlotSource) UnmarshalJSON(data []byte) error {
	type Alias CreateReplicationSlotSource
	s := &struct {
		*Alias
		SlotType string `yaml:"SlotType"`
	}{
		Alias: (*Alias)(c),
	}

	if err := json.Unmarshal(data, &s); err != nil {
		return err
	}

	t, err := ParseReplicationMode(s.SlotType)
	if err != nil {
		return err
	}
	c.SlotType = t

	return nil
}
