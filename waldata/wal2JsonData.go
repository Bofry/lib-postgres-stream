package waldata

import (
	"bytes"
	"encoding/json"
	"errors"

	"github.com/Bofry/structproto"
	"github.com/Bofry/structproto/valuebinder"
)

type Wal2JsonData struct {
	Kind   string `json:"kind"`
	Schema string `json:"schema"`
	Table  string `json:"table"`
	Fields map[string]Wal2JsonDataField
	Keys   map[string]Wal2JsonDataField
}

func (w *Wal2JsonData) UnmarshalJSON(data []byte) error {
	type Alias Wal2JsonData
	v := &struct {
		ColumnNames  []string      `json:"columnnames"`
		ColumnTypes  []string      `json:"columntypes"`
		ColumnValues []interface{} `json:"columnvalues"`
		OldKeys      struct {
			KeyNames  []string      `json:"keynames"`
			KeyTypes  []string      `json:"keytypes"`
			KeyValues []interface{} `json:"keyvalues"`
		} `json:"oldkeys"`
		*Alias
	}{
		Alias: (*Alias)(w),
	}

	buf := bytes.NewBuffer(data)
	decoder := json.NewDecoder(buf)
	decoder.UseNumber()
	if err := decoder.Decode(&v); err != nil {
		return err
	}

	// pack columns
	{
		if len(v.ColumnNames) != len(v.ColumnValues) {
			return errors.New("column number of supplied values do not match column name")
		}
		if len(v.ColumnNames) != len(v.ColumnTypes) {
			return errors.New("column number of supplied types do not match column name")
		}

		if v.ColumnNames != nil {
			v.Fields = make(map[string]Wal2JsonDataField, len(v.ColumnNames))
			for i, name := range v.ColumnNames {
				v.Fields[name] = Wal2JsonDataField{
					Name:  name,
					Type:  v.ColumnTypes[i],
					Value: v.ColumnValues[i],
				}
			}
		}
	}

	// pack keys
	{
		if len(v.OldKeys.KeyNames) != len(v.OldKeys.KeyValues) {
			return errors.New("key number of supplied values do not match key name")
		}
		if len(v.OldKeys.KeyNames) != len(v.OldKeys.KeyTypes) {
			return errors.New("key number of supplied types do not match key name")
		}

		if v.OldKeys.KeyNames != nil {
			v.Keys = make(map[string]Wal2JsonDataField, len(v.OldKeys.KeyNames))
			for i, name := range v.OldKeys.KeyNames {
				v.Keys[name] = Wal2JsonDataField{
					Name:  name,
					Type:  v.OldKeys.KeyTypes[i],
					Value: v.OldKeys.KeyValues[i],
				}
			}
		}
	}
	return nil
}

func (w *Wal2JsonData) FillFields(v interface{}) error {
	if len(w.Fields) == 0 {
		return nil
	}
	return w.bindDataFields(w.Fields, v)
}

func (w *Wal2JsonData) FillKeys(v interface{}) error {
	if len(w.Keys) == 0 {
		return nil
	}
	return w.bindDataFields(w.Keys, v)
}

func (w *Wal2JsonData) bindDataFields(m map[string]Wal2JsonDataField, v interface{}) error {
	st, err := structproto.Prototypify(v, &structproto.StructProtoResolveOption{
		TagName: TAG_KEY,
	})
	if err != nil {
		return err
	}

	ch := w.makeBindChan(m)
	return st.BindChan(ch, valuebinder.BuildScalarBinder)
}

func (w *Wal2JsonData) makeBindChan(m map[string]Wal2JsonDataField) <-chan structproto.FieldValueEntity {
	c := make(chan structproto.FieldValueEntity, 1)
	go func() {
		defer close(c)

		for k, v := range m {
			c <- structproto.FieldValueEntity{
				Field: k,
				Value: v.Value,
			}
		}
	}()
	return c
}
