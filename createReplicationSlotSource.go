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
func (s *CreateReplicationSlotSource) UnmarshalJSON(data []byte) error {
	type Alias CreateReplicationSlotSource
	dummy := &struct {
		*Alias
		SlotType string `yaml:"SlotType"`
	}{
		Alias: (*Alias)(s),
	}

	if err := json.Unmarshal(data, &dummy); err != nil {
		return err
	}

	t, err := ParseReplicationMode(dummy.SlotType)
	if err != nil {
		return err
	}
	s.SlotType = t

	return nil
}

func (s *CreateReplicationSlotSource) AsProvider() *CreateReplicationSlotSourceProvider {
	provider := new(CreateReplicationSlotSourceProvider)
	provider.AppendSource(*s)
	return provider
}
