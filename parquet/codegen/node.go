package codegen

import (
	"fmt"
	"github.com/viant/toolbox"
	"strings"
)

type Node struct {
	OwnerType     string
	OwnerPath     string
	Def           int
	Rep           int
	Pos           int
	Field         *toolbox.FieldInfo
	FieldName     string
	Parent        *Node
	optional      bool
	schemaOptions string
}

func (n *Node) CheckValue() string {
	checkValue := " == nil"
	if !n.Field.IsPointer {
		switch n.Field.TypeName {
		case "string":
			checkValue = ` == ""`
		case "bool":
			checkValue = ` == false`
		case "time.Time":
			checkValue = `.IsZero()`
		case "time.StringTime", "time.StringDate":
			checkValue = `== ""`
		default:
			checkValue = " == 0"

		}
	}
	return checkValue
}

func (n *Node) StructType(maxDef int) string {
	structType := strings.Title(n.ParquetType())
	if maxDef > 0 {
		structType += "Optional"
	}
	return structType + "Field"
}

func (n *Node) IsOptional() bool {
	return n.IsRepeated() || n.Field.IsPointer || n.optional
}

func (n *Node) IsRepeated() bool {
	return n.Field.IsSlice && n.Field.TypeName != "[]byte"
}

func (n *Node) Path() string {
	if n.OwnerPath == "" {
		return n.Field.Name
	}
	return fmt.Sprintf("%v.%v", n.OwnerPath, n.Field.Name)
}

func (n *Node) RelativePath() string {
	if n.OwnerPath == "" {
		return n.Field.Name
	}
	ownerPath := n.OwnerPath
	if strings.HasPrefix(ownerPath, "v.") {
		ownerPath = ownerPath[2:]
	} else if ownerPath == "v" {
		return n.Field.Name
	}
	return fmt.Sprintf("%v.%v", ownerPath, n.Field.Name)
}

func (n *Node) CastParquetBegin() string {
	simpleType := n.SimpleType()
	mapped, ok := parquetTypeMapping[simpleType]
	if ok {
		if strings.HasSuffix(n.Field.TypeName, "time.Time") {
			return "("
		} else if strings.HasSuffix(n.Field.TypeName, "time.StringTime") {
			return "parquet.StringToTime("
		} else if strings.HasSuffix(n.Field.TypeName, "time.StringDate") {
			return "parquet.StringToDate("
		}
		return mapped + "("
	}
	return ""
}

func (n *Node) CastParquetEnd() string {
	simpleType := n.SimpleType()
	_, ok := parquetTypeMapping[simpleType]
	if ok {
		if strings.HasSuffix(n.Field.TypeName, "time.Time") {
			return fmt.Sprintf(").UnixNano()/1000000")
		} else if strings.HasSuffix(n.Field.TypeName, "time.StringTime") {
			return fmt.Sprintf(").UnixNano()/1000000")
		} else if strings.HasSuffix(n.Field.TypeName, "time.StringDate") {
			return fmt.Sprintf(")")
		}
		return ")"
	}
	return ""
}

func (n *Node) CastNativeBegin() string {
	simpleType := n.SimpleType()
	if _, ok := parquetTypeMapping[simpleType]; !ok {
		return ""
	}
	if strings.HasSuffix(n.Field.TypeName, "time.Time") {
		return "time.Unix(0, "
	} else if strings.HasSuffix(n.Field.TypeName, "time.StringTime") {
		return "parquet.TimeToString(time.Unix(0, "
	} else if strings.HasSuffix(n.Field.TypeName, "time.StringDate") {
		return "parquet.DateToString("
	}
	return simpleType + "("
}

func (n *Node) CastNativeEnd() string {
	simpleType := n.SimpleType()
	if _, ok := parquetTypeMapping[simpleType]; !ok {
		return ""
	}
	if strings.HasSuffix(n.Field.TypeName, "time.Time") {
		return ")"
	} else if strings.HasSuffix(n.Field.TypeName, "time.StringTime") {
		return "))"
	} else if strings.HasSuffix(n.Field.TypeName, "time.StringDate") {
		return ")"
	}
	return ")"
}

func (n *Node) SimpleType() string {
	if n.Field.ComponentType != "" && n.Field.TypeName != "[]byte" {
		return n.Field.ComponentType
	}
	return n.Field.TypeName
}

func (n *Node) ParquetType() string {
	return lookupParquetType(n.SimpleType())
}

func NewNode(sess *session, ownerType string, field *toolbox.FieldInfo) *Node {
	node := &Node{
		OwnerType: ownerType,
		Field:     field,
		FieldName: field.Name,
	}
	tagItems := getTagOptions(field.Tag, PARQUET_KEY)
	if tagItems != nil {
		if _, ok := tagItems["name"]; ok {
			node.FieldName = tagItems["name"]
		}
	}
	node.setOptions()
	return node
}

const (
	tagLogicalType   = "logicalType"
	tagConvertedType = "convertedType"
)

func (n *Node) setOptions() {

	tagItems := getTagOptions(n.Field.Tag, PARQUET_KEY)
	var options = make([]string, 0)
	convertedType := tagItems[tagConvertedType]
	normalizedType := normalizeTypeName(n.Field.TypeName)

	if convertedType == strings.ToUpper(convertedType) && convertedType != "UTF8" {
		convertedType = toolbox.ToCaseFormat(convertedType, toolbox.CaseUpperUnderscore, toolbox.CaseUpperCamel)
	}

	if convertedType == "TimestampMillis" && n.Field.TypeName == "string" {
		n.Field.TypeName = "time.StringTime"
	}

	logicalType := tagItems[tagLogicalType]

	if convertedType == "Date" && n.Field.TypeName == "string" {
		n.Field.TypeName = "time.StringDate"
		if logicalType == "" {
			logicalType = "Date"
		}
	}

	if convertedType == "" {
		switch normalizedType {
		case "string":
			convertedType = "UTF8"
		case "time.Time":
			convertedType = "TimestampMillis"
			logicalType = "TimestampMillis"
		}
	}
	if convertedType != "" {
		options = append(options, fmt.Sprintf(`parquet.ConvertedType%v`, convertedType))
	}

	if logicalType == strings.ToUpper(logicalType) {
		logicalType = toolbox.ToCaseFormat(logicalType, toolbox.CaseUpperUnderscore, toolbox.CaseUpperCamel)
	}

	if logicalType == "" {
		switch normalizedType {
		case "string":
			logicalType = "String"
		case "time.Time":
			logicalType = "TimestampMillis"
		}
	}
	if logicalType != "" {
		options = append(options, fmt.Sprintf(`parquet.LogicalType%v`, logicalType))
	}
	n.schemaOptions = strings.Join(options, ",")
}
