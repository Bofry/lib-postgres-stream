package postgres

import (
	"encoding/json"
	"os"
	"unsafe"
)

type CreateReplicationSlotSourceProvider struct {
	addr *CreateReplicationSlotSourceProvider

	sources []CreateReplicationSlotSource
}

func (p *CreateReplicationSlotSourceProvider) copyCheck() {
	if p.addr == nil {
		p.addr = (*CreateReplicationSlotSourceProvider)(unsafe.Pointer(p))
	} else if p.addr != p {
		panic("lib-postgres-stream: illegal use of non-zero CreateReplicationSlotSourceProvider copied by value")
	}
}

func (p *CreateReplicationSlotSourceProvider) Append(provider *CreateReplicationSlotSourceProvider) error {
	p.copyCheck()

	p.sources = append(p.sources, provider.sources...)
	return nil
}

func (p *CreateReplicationSlotSourceProvider) AppendSource(source CreateReplicationSlotSource) error {
	p.copyCheck()

	p.sources = append(p.sources, source)
	return nil
}

func (p *CreateReplicationSlotSourceProvider) Scan(buf []byte) error {
	p.copyCheck()

	var source []CreateReplicationSlotSource
	err := json.Unmarshal([]byte(buf), &source)
	if err != nil {
		return err
	}
	p.sources = append(p.sources, source...)
	return nil
}

func (p *CreateReplicationSlotSourceProvider) ScanFile(filepath string) error {
	p.copyCheck()

	buf, err := os.ReadFile(filepath)
	if err != nil {
		return err
	}

	return p.Scan(buf)
}

func (p *CreateReplicationSlotSourceProvider) Sources() []CreateReplicationSlotSource {
	return p.sources
}
