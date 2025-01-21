package postgres

type Wal2JsonDataField struct {
	Name  string
	Type  string
	Value interface{}
}

func (f Wal2JsonDataField) IsInvalid() bool {
	return len(f.Name) == 0
}
