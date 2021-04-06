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

/*
	Root->R(s)->S1(s)->L(s)
  ->  R.S1.L1
  ->    item.L1



*/

func (n Nodes) MutatorOwnerPath(endNodePos int) string {
	var elements = make([]string, 0)
	if endNodePos == 0 {
		return "v"
	}
	elements = append(elements, "v")
	for i := 1; i < endNodePos; i++ {
		elements = append(elements, n[i].Field.Name)
	}
	return strings.Join(elements, ".")
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

func (n Nodes) DefCases() []string {
	var result = make([]string, 0)
	d := 1
	for i := 0; i < len(n)-1; i++ {
		item := n[i]
		if !item.Field.IsSlice {
			continue
		}
		path := n.MutatorOwnerPath(i) + "." + item.Field.Name
		init := fmt.Sprintf("    %v = append(%v, %v{})", path, path, item.Field.ComponentType)
		result = append(result, init)
		d++
	}
	result = append(result, "TODO add me")
	return result
}


/*
case 1:
			x.B = append(x.B, B{})
		case 2:
			x.B = append(x.B, B{C: []C{{}}})
		case 3:

			switch rep {
			case 0:
				x.B = []B{{C: []C{{S: []string{vals[nVals]}}}}}
			case 1:
				x.B = append(x.B, B{C: []C{{S: []string{vals[nVals]}}}})
			case 2:
				x.B[ind[0]].C = append(x.B[ind[0]].C, C{S: []string{vals[nVals]}})
			case 3:
				x.B[ind[0]].C[ind[1]].S = append(x.B[ind[0]].C[ind[1]].S, vals[nVals])
			}
			nVals++
*/
