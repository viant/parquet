package codegen

import (
	"fmt"
	"github.com/viant/toolbox"
	"strings"
)

type Node struct {
	OwnerType string
	OwnerPath string
	Def       int
	Rep       int
	Pos       int
	Field     *toolbox.FieldInfo
	FieldName string
	FieldType string
	Parent    *Node
	optional  bool
}


func (n *Node) CheckValue() string {
	checkValue := "nil"
	if !n.Field.IsPointer {
		switch n.Field.TypeName {
		case "string":
			checkValue = `""`
		case "bool":
			checkValue = `false`
		default:
			checkValue = "0"

		}
	}
	return checkValue
}

func (n *Node) StructType() string {
	structType := strings.Title(n.ParquetType())
	if n.IsOptional() {
		structType += "Optional"
	}
	return structType + "Field"
}

func (n *Node) IsOptional() bool {
	return n.Field.IsSlice || n.Field.IsPointer || n.optional
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
		return mapped + "("
	}
	return ""
}

func (n *Node) CastParquetEnd() string {
	simpleType := n.SimpleType()
	_, ok := parquetTypeMapping[simpleType]
	if ok {
		return ")"
	}
	return ""
}

func (n *Node) CastNativeBegin() string {
	simpleType := n.SimpleType()
	if _, ok := parquetTypeMapping[simpleType]; !ok {
		return ""
	}
	return simpleType + "("
}

func (n *Node) CastNativeEnd() string {
	simpleType := n.SimpleType()
	if _, ok := parquetTypeMapping[simpleType]; !ok {
		return ""
	}
	return ")"
}

func (n *Node) SimpleType() string {
	if n.Field.ComponentType != "" {
		return n.Field.ComponentType
	}
	return normalizeTypeName(n.Field.TypeName)
}

func (n *Node) ParquetType() string {
	return lookupParquetType(n.SimpleType())
}

func (n *Node) Indent() int {
	return n.Pos
}

//
//func (n *Node) NewParams() *FieldParams {
//	param := NewFieldParams(n)
//	param.OwnerPath = n.OwnerPath
//	return param
//}

func NewNode(sess *session, ownerType string, field *toolbox.FieldInfo) *Node {
	node := &Node{
		OwnerType: ownerType,
		Field:     field,
		FieldName: field.Name,
		FieldType: field.TypeName,
	}
	tagItems := getTagOptions(field.Tag, PARQUET_KEY)
	if tagItems != nil {
		node.FieldName = tagItems[0]
	}
	return node
}
