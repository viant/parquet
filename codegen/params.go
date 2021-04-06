package codegen

import (
	"fmt"
	"strings"
)

//FieldParams represents template variables
type FieldParams struct {
	OwnerType              string
	OwnerAlias             string
	FieldName              string
	MethodSuffix           string
	FieldType              string
	FieldStructType        string // []Int32
	ParquetType            string //[]int32
	SimpleMethodRoot       string //Int32
	SimpleLowerParquetType string //int32
	ChildSnippet           string
	PosVar                 string
	ItemVar                string
	ParentType             string
	Depth                  int
	NilDepth               int
	Indent                 string
	OwnerPath              string
	DefCases               string
	RepCases               string
}

func (p *FieldParams) SetIndent(n int) {
	p.Indent = strings.Repeat(" ", n)
}

func NewFieldParams(node *Node) *FieldParams {
	alias := strings.ToLower(node.OwnerType[0:1])
	parquetType := lookupParquetType(node.FieldType)
	methodRoot := parquetType
	if node.Field.ComponentType != "" {
		methodRoot = lookupParquetType(node.Field.ComponentType)
	}
	if node.IsOptional {
		methodRoot += "Optional"
	}
	methodRoot = strings.Title(methodRoot)
	return &FieldParams{
		OwnerType:       node.OwnerType,
		OwnerAlias:      alias,
		FieldName:       node.FieldName,
		FieldType:       node.FieldType,
		ParquetType:     parquetType,
		FieldStructType: methodRoot + "Field",
		Depth:           node.Depth,
		NilDepth:        node.Depth - 1,
		PosVar:          fmt.Sprintf("i%v", node.Pos),
		ItemVar:         fmt.Sprintf("v%v", node.Pos),
	}
}
