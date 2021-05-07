package codegen

import (
	"fmt"
	"github.com/viant/toolbox"
	"reflect"
	"strings"
)

const (
	PARQUET_KEY = "parquet"
)

func getTagOptions(tag, key string) map[string]string {
	if tag == "" {
		return nil
	}
	var structTag = reflect.StructTag(strings.Replace(tag, "`", "", len(tag)))
	options, ok := structTag.Lookup(key)
	if !ok {
		return nil
	}
	var result = make(map[string]string)
	for _, item := range strings.Split(options, ",") {
		pair := strings.Split(item, "=")
		switch len(pair) {
		case 1:
			result["name"] = strings.TrimSpace(pair[0])
		default:
			result[strings.TrimSpace(pair[0])] = strings.TrimSpace(pair[1])
		}
	}
	return result
}

func getRequiredFieldInit(nodes Nodes) string {
	params := nodes.NewParams("")
	return fmt.Sprintf(`New%v(read%v, write%v,[]string{%v}, fieldCompression(compression), parquet.SchemaOption(%v)),`,
		params.StructType, params.MethodSuffix, params.MethodSuffix, nodes.PathList(), nodes.SchemaOptions(),
	)
}

func getOptionalFieldInit(nodes Nodes) string {

	params := nodes.NewParams("")
	return fmt.Sprintf(`New%v(read%v, write%v,[]string{%v},[]int{%v}, optionalFieldCompression(compression), parquet.OptionalSchemaOption(%v)),`,
		params.StructType, nodes.MethodSuffix(), nodes.MethodSuffix(), nodes.PathList(), nodes.RepetitionTypesList(), nodes.SchemaOptions(),
	)
}

func normalizeTypeName(name string) string {
	for _, seq := range []string{"[]", "*"} {
		count := strings.Count(name, seq)
		if count == 0 {
			continue
		}
		name = strings.Replace(name, seq, "", count)
	}
	return name
}

func allocLeafSnippet(field *toolbox.FieldInfo, append bool) string {
	init := ""
	if field.IsSlice && !append {
		init = "{}"
	}
	return fmt.Sprintf("%v{%v}", qualifiedType(field, append), init)
}

func qualifiedType(field *toolbox.FieldInfo, append bool) string {
	modifier := ""
	if append {
		if field.IsPointer || field.IsPointerComponent {
			modifier += "&"
		}
	} else {
		if field.IsSlice {
			modifier += "[]"
		}

		if field.IsPointerComponent {
			modifier += "*"
		}
		if field.IsPointer {
			if field.IsSlice {
				modifier += "*"
			} else {
				modifier += "&"
			}
		}
	}
	typeName := field.TypeName
	if field.ComponentType != "" {
		typeName = field.ComponentType
	}
	return modifier + normalizeTypeName(typeName)
}

func allocNodeSnippet(owner, child *toolbox.FieldInfo, init string, append bool) string {
	ownerType := qualifiedType(owner, append)
	itemStart := ""
	itemEnd := ""
	if !append && owner.IsSlice {
		itemStart = "{"
		itemEnd = "}"
	}
	return fmt.Sprintf("%v%v{%v: %v}%v", ownerType, itemStart, child.Name, init, itemEnd)
}
