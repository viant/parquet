package codegen

import (
	"bytes"
	"fmt"
	"github.com/viant/toolbox"
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

//NewInt32Field(readID, writeID, []string{"id"}, fieldCompression(compression)),
//NewInt32OptionalField(readAge, writeAge, []string{"age"}, []int{1}, optionalFieldCompression(compression)),

func getRequiredFieldInit(fieldPath []string, field *toolbox.FieldInfo) string {
	var fieldName = field.Name
	tagItems := getTagOptions(field.Tag, PARQUET_KEY)
	if tagItems != nil {
		fieldName = tagItems[0]
	}

	parquetType := lookupParquetType(field.TypeName)
	camelParquetType := strings.Title(parquetType)
	return fmt.Sprintf(`New%vField(read%v, write%v,[]string{%v}, fieldCompression(compression)),`,
		camelParquetType, field.Name, field.Name, inlineQuotedSlice(fieldPath, fieldName),
	)
}

func getOptionalFieldInit(field *toolbox.FieldInfo) string {
	var parquetName = field.Name
	tagItems := getTagOptions(field.Tag, PARQUET_KEY)
	if tagItems != nil {
		parquetName = tagItems[0]
	}
	aFieldType := strings.Title(field.TypeName)
	if field.IsSlice {
		aFieldType = strings.Title(field.ComponentType)
	}
	var reps = getRepNumber(field)
	var buffer bytes.Buffer
	buffer.WriteString("New" + aFieldType + "OptionalField(read" + field.Name + ",write" + field.Name + ",[]string{\"" + parquetName + "\"}," + reps + ",optionalFieldCompression(compression)),")
	return buffer.String()
}

func inlineQuotedSlice(path []string, name string) string {
	if len(path) == 0 {
		path = []string{}
	}
	var items = append(path, name)
	for i, item := range items {
		items[i] = `"` + item + `"`
	}
	return strings.Join(items, ",")
}

func getRepNumber(field *toolbox.FieldInfo) string {
	if field.IsSlice {
		return "[]int{2}"
	}
	return "[]int{1}"
}
