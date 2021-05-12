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
	m := map[string]elementGraph{"": {SchemaElement: out[0], children: make(map[string]elementGraph)}}

	for _, f := range s.fields {
		if len(f.Path) > 1 {
			for i, name := range f.Path[:len(f.Path)-1] {
				parent := ""
				if i > 0 {
					parent = strings.Join(f.Path[:i], "/")
				}
				fPath := strings.Join(f.Path[:i+1], "/")
				par, ok := m[fPath]
				if !ok {
					parts := strings.Split(name, ".")
					rt := sch.FieldRepetitionType(f.Types[i])
					par = elementGraph{
						SchemaElement: &sch.SchemaElement{
							Name:           parts[len(parts)-1],
							RepetitionType: &rt,
							NumChildren:    &z,
						},
						children: make(map[string]elementGraph),
					}
					out = append(out, par.SchemaElement)
					m[fPath] = par
				}

				m[parent].children[name] = m[fPath]
				cN := int32(len(m[parent].children))
				m[parent].SchemaElement.NumChildren = &cN
			}
		} else if len(f.Path) == 1 {
			children++
		}

		parent := ""
		if len(f.Path) > 1 {
			parent = strings.Join(f.Path[:len(f.Path)-1], "/")
		}
		m[parent].children[f.Path[len(f.Path)-1]] = elementGraph{}
		cN := int32(len(m[parent].children))
		m[parent].SchemaElement.NumChildren = &cN

		se := &sch.SchemaElement{
			Name:       f.Path[len(f.Path)-1],
			TypeLength: &z,
			Scale:      &z,
			Precision:  &z,
			FieldID:    &z,
		}
		for _, opt := range f.Options {
			opt(se)
		}
		out = append(out, se)
	}
	//out[0].NumChildren = &children
	return int64(len(s.fields)), out
}

type elementGraph struct {
	*sch.SchemaElement
	children map[string]elementGraph
}

func schemaElements(fields []Field) schema {
	m := make(map[string]sch.SchemaElement)
	for _, f := range fields {
		var z int32
		se := sch.SchemaElement{
			Name:       f.Name,
			TypeLength: &z,
			Scale:      &z,
			Precision:  &z,
			FieldID:    &z,
		}
		for _, opt := range f.Options {
			opt(&se)
		}
		m[strings.Join(f.Path, ".")] = se
	}
	return schema{lookup: m, fields: fields}
}
