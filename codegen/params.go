package codegen

import (
	"fmt"
	"github.com/viant/toolbox"
	"strings"
)

//FieldParams represents template variables
type FieldParams struct {
	OwnerType              string
	OwnerAlias             string
	FieldName              string
	FieldType              string
	UpperParquetType       string // []Int32
	ParquetType            string //[]int32
	SimpleUpperParquetType string //Int32
	SimpleLowerParquetType string //int32
	ChildSnippet           string
	PosVar                 string
	ItemVar                string
	ParentType             string
	ChildName              string
	Depth                  int
	NilDepth               int
	Indent                 string
	OwnerPath              string
}

func (p *FieldParams) SetIndent(n int) {
	p.Indent = strings.Repeat(" ", n)
}

func NewFieldParams(ownerType, ownerAlias, fieldName, fieldType, componentType string, pos, depth int) *FieldParams {
	parquetType := lookupParquetType(fieldType)
	return &FieldParams{
		OwnerType:              ownerType,
		OwnerAlias:             ownerAlias,
		FieldName:              fieldName,
		FieldType:              fieldType,
		ParquetType:            parquetType,
		UpperParquetType:       toolbox.ToCaseFormat(parquetType, toolbox.CaseLowerCamel, toolbox.CaseUpperCamel),
		SimpleLowerParquetType: componentType,
		SimpleUpperParquetType: toolbox.ToCaseFormat(componentType, toolbox.CaseLowerCamel, toolbox.CaseUpperCamel),
		Depth:                  depth,
		NilDepth:               depth - 1,
		PosVar:                 fmt.Sprintf("i%v", pos),
		ItemVar:                fmt.Sprintf("v%v", pos),
	}
}
