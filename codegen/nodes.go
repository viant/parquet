package codegen

import (
	"fmt"
	"github.com/viant/toolbox"
	"strings"
)

type Nodes []*Node

func (n Nodes) MethodSuffix() string {
	var result = make([]string, len(n))
	for i, item := range n {
		result[i] = item.FieldName
	}
	return strings.Join(result, "")
}

func (n Nodes) RepeatedPos(def int) int {
	if def == 0 {
		return -1
	}
	def--
	k := 0
	for i, item := range n {
		if item.Field.IsSlice {
			k++
			if k >= def {
				return i
			}
		}

	}
	return -1
}

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
		(*n)[i].OwnerPath = n.AccessorOwnerPath(i)
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

func (n Nodes) AccessorOwnerPath(endNodePos int) string {
	var elements = make([]string, 0)
	if endNodePos == 0 {
		return "v"
	}
	elements = append(elements, "v")
	for i := 1; i <= endNodePos; i++ {
		node := n[i-1]

		if node.Field.IsSlice {
			elements = []string{}
			elements = append(elements, fmt.Sprintf("v%v", node.Pos))
			continue
		}
		elements = append(elements, node.Field.Name)
	}

	return strings.Join(elements, ".")
}

//DefCaseAppendPath produces accessor to append at depth (def -1)
func (n Nodes) DefCaseAppendPath(caseNo int) string {
	if caseNo == 0 {
		return ""
	}
	depth := caseNo - 1
	result := []string{fmt.Sprintf("x")}
	repeatedCount := 0
	slicePos := -1
	k := 0
	for i, node := range n {
		isLast := k > depth
		isRepeated := node.Field.IsSlice
		if isLast && slicePos >= 0 {
			break
		}

		if isRepeated {
			if repeatedCount > 0 {
				result[slicePos] += fmt.Sprintf("[ind[%v]]", repeatedCount-1)
			}
			slicePos = i
			repeatedCount++
		}
		result = append(result, node.FieldName)
		if isRepeated {
			k++
		}
	}
	return strings.Join(result, ".")
}

func (n Nodes) DefCaseAppendValue(caseNo int) string {
	if caseNo == 0 {
		return ""
	}
	pos := n.RepeatedPos(caseNo)
	if pos == -1 {
		return ""
	}
	var nodes = make(Nodes, 0)
	nodes = append(nodes, n[pos])
	if pos+1 < len(n) {
		nodes = append(nodes, n[pos+1])
	}
	depth := caseNo - 1
	modifier := ""
	init := ""
	if depth == 0 || len(nodes) == 1 {
		if len(nodes) > 1 && !nodes[1].Field.IsSlice {
			modifier = "[]"
			init = "{}"
		}
		return fmt.Sprintf(`%v%v{%v}`, modifier, nodes[0].Field.Name, init)
	}
	if nodes[1].Field.IsSlice {
		modifier = "[]"
		init = "{}"
	}
	return fmt.Sprintf(`%v{%v: %v%v{%v}}`, nodes[0].Field.Name, nodes[1].Field.Name,
		modifier, nodes[1].Field.Name, init)
}

func (n Nodes) DefCases(indent int) []string {

	
	indentSpace := strings.Repeat(" ", indent)
	var result = make([]string, 0)
	d := 1
	for i := 0; i < len(n)-1; i++ {
		item := n[i]
		if !item.Field.IsSlice {
			continue
		}
		path := ""
		init := fmt.Sprintf(indentSpace+"%v = append(%v, %v)", path, path, item.Field.ComponentType)
		result = append(result, init)
		d++
	}
	result = append(result, "TODO add me")
	return result
}
