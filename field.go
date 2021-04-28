package parquet

import sch "github.com/viant/parquet/schema"

// Field holds the type information for a parquet column
type Field struct {
	Name           string
	Path           []string
	Types          []int
	Type           FieldFunc
	RepetitionType FieldFunc
	ConvertedType  *sch.ConvertedType
	LogicalType    *sch.LogicalType
}

