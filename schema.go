package parquet

import (
	sch "github.com/viant/parquet/schema"
	"strings"
)

type schema struct {
	fields []Field
	lookup map[string]sch.SchemaElement
}

func (s schema) schema() (int64, []*sch.SchemaElement) {
	out := make([]*sch.SchemaElement, 0, len(s.fields)+1)
	out = append(out, &sch.SchemaElement{
		Name: "root",
	})

	var children int32
	var z int32
	m := map[string]*sch.SchemaElement{}
	for _, f := range s.fields {
		if len(f.Path) > 1 {
			for i, name := range f.Path[:len(f.Path)-1] {
				par, ok := m[name]
				if !ok {
					children++
					parts := strings.Split(name, ".")
					rt := sch.FieldRepetitionType(f.Types[i])
					par = &sch.SchemaElement{
						Name:           parts[len(parts)-1],
						RepetitionType: &rt,
						NumChildren:    &z,
						LogicalType:    f.LogicalType,
						ConvertedType:  f.ConvertedType,
					}
					out = append(out, par)
				}
				n := *par.NumChildren
				n++
				par.NumChildren = &n
				m[name] = par
			}
		} else if len(f.Path) == 1 {
			children++
		}

		se := &sch.SchemaElement{
			Name:       f.Path[len(f.Path)-1],
			TypeLength: &z,
			Scale:      &z,
			Precision:  &z,
			FieldID:    &z,
			LogicalType: f.LogicalType,
			ConvertedType: f.ConvertedType,
		}

		f.Type(se)
		f.RepetitionType(se)
		out = append(out, se)
	}

	out[0].NumChildren = &children
	return int64(len(s.fields)), out
}


func schemaElements(fields []Field) schema {
	m := make(map[string]sch.SchemaElement)
	for _, f := range fields {
		var z int32
		se := sch.SchemaElement{
			Name:          f.Name,
			TypeLength:    &z,
			Scale:         &z,
			Precision:     &z,
			FieldID:       &z,
			LogicalType:   f.LogicalType,
			ConvertedType: f.ConvertedType,
		}
		f.Type(&se)
		f.RepetitionType(&se)
		m[strings.Join(f.Path, ".")] = se
	}
	return schema{lookup: m, fields: fields}
}
