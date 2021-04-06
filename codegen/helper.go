package codegen

import (
	"fmt"
	"reflect"
	"strings"
)

const (
	PARQUET_KEY = "parquet"
)

func getTagOptions(tag, key string) []string {
	if tag == "" {
		return nil
	}
	var structTag = reflect.StructTag(strings.Replace(tag, "`", "", len(tag)))
	options, ok := structTag.Lookup(key)
	if !ok {
		return nil
	}
	return strings.Split(options, ",")
}

func getRequiredFieldInit(nodes Nodes) string {
	node := nodes.Leaf()
	params := node.NewParams()
	return fmt.Sprintf(`New%vField(read%v, write%v,[]string{%v}, fieldCompression(compression)),`,
		params.UpperParquetType, node.FieldName, node.FieldName, nodes.PathList(),
	)
}

func getOptionalFieldInit(nodes Nodes) string {
	node := nodes.Leaf()
	params := node.NewParams()
	return fmt.Sprintf(`New%vOptionalField(read%v, write%v,[]string{%v},[]int{%v} fieldCompression(compression)),`,
		params.UpperParquetType, node.FieldName, node.FieldName, nodes.PathList(),nodes.RepetitionTypesList(),
	)
}

func normalizeTypeName(name string) string {
	for _, seq := range[]string {"[]","*"} {
		count := strings.Count(name, seq)
		if count == 0 {
			continue
		}
		name = strings.Replace(name, seq, "",  count)
	}
	return name
}
