package codegen

import "strings"

var parquetTypeMapping = map[string]string{
	"int":    "int64",
	"uint":   "int64",
	"uint64": "int64",
	"int16":  "int32",
	"uint16": "int32",
	"uint32": "int32",
}

func lookupParquetType(typeName string) string {
	typeName = strings.Replace(typeName, "*", "", len(typeName))
	mapped, ok := parquetTypeMapping[typeName]
	if !ok {
		return typeName
	}
	return mapped
}
