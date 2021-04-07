package codegen

import (
	"github.com/viant/toolbox"
)

type Node struct {
	OwnerType string
	OwnerPath string
	MaxDef    int
	Depth     int
	Pos        int
	Field      *toolbox.FieldInfo
	FieldName  string
	FieldType  string
	IsOptional bool
	Parent     *Node
}

func (n *Node) Indent() int {
	return n.Depth
}

func (n *Node) NewParams() *FieldParams {
	param := NewFieldParams(n)
	param.OwnerPath = n.OwnerPath
	return param
}

func NewNode(sess *session, ownerType string, field *toolbox.FieldInfo) *Node {
	node := &Node{
		OwnerType: ownerType,
		Field:     field,

		IsOptional: field.IsPointer || sess.OmitEmpty || field.IsSlice,
		FieldName:  field.Name,
		FieldType:  field.TypeName,
	}
	tagItems := getTagOptions(field.Tag, PARQUET_KEY)
	if tagItems != nil {
		node.FieldName = tagItems[0]
	}
	return node
}
