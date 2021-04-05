package codegen

import (
	"github.com/viant/toolbox"
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
}

func NewFieldParams(ownerType, ownerAlias, fieldName, fieldType, componentType string) *FieldParams {
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
	}
}
