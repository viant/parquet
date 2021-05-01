package codegen

import (
	"strings"
)

var parquetTypeMapping = map[string]string{
	"int":        "int64",
	"uint":       "int64",
	"uint64":     "int64",
	"int16":      "int32",
	"uint16":     "int32",
	"uint32":     "int32",
	"[]byte":     "string",
	"time.Time":  "int64",
	"*time.Time": "int64",
	"Time":       "int64",
}

func lookupParquetType(typeName string) string {
	typeName = strings.Replace(typeName, "*", "", len(typeName))
	if index := strings.LastIndex(typeName, "."); index != -1 {
		typeName = typeName[index+1:]
	}
	mapped, ok := parquetTypeMapping[typeName]
	if !ok {
		mapped, ok = parquetTypeMapping[normalizeTypeName(typeName)]
		if !ok {
			return normalizeTypeName(typeName)
		}
	}
	return mapped
}
