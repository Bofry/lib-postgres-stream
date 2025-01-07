package postgres

import (
	"encoding/json"
	"os"
)

type CreateReplicationSlotSourceProvider struct {
	sources []CreateReplicationSlotSource
}

func (p *CreateReplicationSlotSourceProvider) Append(provider *CreateReplicationSlotSourceProvider) error {
	p.sources = append(p.sources, provider.sources...)
	return nil
}

func (p *CreateReplicationSlotSourceProvider) AppendSource(source CreateReplicationSlotSource) error {
	p.sources = append(p.sources, source)
	return nil
}

func (p *CreateReplicationSlotSourceProvider) Scan(buf []byte) error {
	var source []CreateReplicationSlotSource
	err := json.Unmarshal([]byte(buf), &source)
	if err != nil {
		return err
	}
	p.sources = append(p.sources, source...)
	return nil
}

func (p *CreateReplicationSlotSourceProvider) ScanString(text string) error {
	return p.Scan([]byte(text))
}

func (p *CreateReplicationSlotSourceProvider) ScanFile(filepath string) error {
	buf, err := os.ReadFile(filepath)
	if err != nil {
		return err
	}

	return p.Scan(buf)
}

func (p *CreateReplicationSlotSourceProvider) Sources() []CreateReplicationSlotSource {
	return p.sources
}
