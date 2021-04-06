package codegen

import (
	"fmt"
	"github.com/viant/toolbox"
	"strings"
)

type Node struct {
	OwnerType  string
	Depth      int
	Pos        int
	Field      *toolbox.FieldInfo
	FieldName  string
	FieldType  string
	IsOptional bool
	Parent     *Node
}

func (n *Node) NewParams() *FieldParams {
	alias := strings.ToLower(n.OwnerType[0:1])
	param :=  NewFieldParams(n.OwnerType, alias, n.Field.Name, n.Field.TypeName, n.Field.ComponentType, n.Pos, n.Depth)
	param.OwnerPath  = n.OwnerPath()
	return param
}

func (n *Node) OwnerPath() string {
	var elements = make([]string, 0)
	node := n.Parent
	for {
		if node == nil {
			elements = append(elements, "v")
			break
		}
		if node.Field.IsSlice {
			elements = append(elements, fmt.Sprintf("v%v",n.Pos-1))
			break
		}
		elements = append(elements, node.Field.Name)
		node = node.Parent
	}

	return strings.Join(elements, ".")
}

func NewNode(sess *session, ownerType string, field *toolbox.FieldInfo) *Node {
	node := &Node{
		OwnerType:  ownerType,
		Field:      field,
		IsOptional: field.IsPointer || sess.OmitEmpty || field.IsSlice,
		FieldName:  field.Name,
		FieldType:  field.TypeName,
	}
	if field.ComponentType != "" {
		node.FieldType = field.ComponentType
	}
	tagItems := getTagOptions(field.Tag, PARQUET_KEY)
	if tagItems != nil {
		node.FieldName = tagItems[0]
	}
	return node
}



type Nodes []*Node

func (n Nodes) Leaf() *Node {
	return n[len(n)-1]
}

func (n Nodes) RepetitionTypes() []int {
	var result = make([]int, len(n))
	for i, item := range n {
		if item.Field.IsPointer {
			result[i] = 1
		}
		if item.Field.IsSlice {
			result[i] = 2
		}
	}
	return result
}

func (n *Nodes) Init() {
	depth := 0
	for i, item := range *n {
		if item.IsOptional {
			depth++
			(*n)[i].Depth = depth
		}
		(*n)[i].Pos = i
		if i > 0 {
			(*n)[i].Parent = (*n)[i-1]
		}
	}
}

func (n Nodes) RepetitionTypesList() string {
	var reps = make([]string, len(n))
	for i, item := range n.RepetitionTypes() {
		reps[i] = toolbox.AsString(item)
	}
	return strings.Join(reps, ",")
}

func (n Nodes) Path() []string {
	var result = make([]string, len(n))
	for i, item := range n {
		result[i] = item.FieldName
	}
	return result
}

func (n Nodes) PathList() string {
	items := n.Path()
	for i, item := range items {
		items[i] = `"` + item + `"`
	}
	return strings.Join(items, ",")
}
