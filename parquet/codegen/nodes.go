package codegen

import (
	"fmt"
	"github.com/viant/parquet"
	"github.com/viant/toolbox"
	"strings"
)

type Nodes []*Node

func (n Nodes) MaxDef() int {
	maxDef := 0
	for _, item := range n {
		if item.IsOptional() {
			maxDef++
		}
	}
	return maxDef
}

func (n Nodes) OwnerType() string {
	return n[0].OwnerType
}

func (n Nodes) MaxRep() int {
	maxRep := 0
	for _, item := range n {
		if item.IsRepeated() {
			maxRep++
		}
	}
	return maxRep
}

func (n Nodes) Names() []string {
	var result = make([]string, 0)
	for _, item := range n {
		result = append(result, item.Field.Name)
	}
	return result
}

func (n Nodes) MethodSuffix() string {
	var result = make([]string, len(n))
	for i, item := range n {
		result[i] = item.FieldName
	}
	return strings.Join(result, "")
}

func (n Nodes) DefNilNode(def int) *Node {
	k := 0
	var item *Node
	for _, item = range n {
		if item.IsOptional() {
			k++
			if k > def {
				return item
			}

		}
	}
	return item
}

func (n Nodes) DefPos(def int) int {
	if def == 0 {
		return -1
	}
	k := 0
	for i, item := range n {
		if item.IsOptional() {
			k++
			if k >= def {
				return i
			}
		}
	}
	return -1
}

func (n Nodes) RepPos(rep int) int {
	k := -1
	for i, item := range n {
		if item.IsRepeated() {
			k++
			if k == rep {
				return i
			}
		}
	}
	return -1
}

//DefCasePathPos returns the node position where supplied is equal or less to node rep, or -1 otherwise
func (n Nodes) DefCasePathPos(def int) int {
	nDef := 0
	nRep := 0
	firstOptional := -1
	var slicePos = make([]int, 0)
	for i, node := range n {
		if node.IsOptional() {
			if firstOptional == -1 {
				firstOptional = i
			}
			nDef++
		}
		if node.IsRepeated() {
			nRep++
			slicePos = append(slicePos, i)
		}
		if nDef >= def {
			break
		}
	}
	switch len(slicePos) {
	case 0:
		fallthrough
	case 1:
		return firstOptional
	case 2:
		return slicePos[0]
	}
	return slicePos[len(slicePos)-2]
}

//DefCaseAppendPath produces optional/repeated case mutator path
func (n Nodes) DefCasePath(def int) string {
	result := []string{fmt.Sprintf("v")}
	pos := n.DefCasePathPos(def)
	if pos == -1 {
		pos = 0
	}
	n.nodePath(pos, &result)
	return strings.Join(result, ".")
}

func (n Nodes) nodePath(pos int, result *[]string) {
	rep := 0
	slicePos := 0
	for i := 0; i <= pos; i++ {
		node := n[i]

		if node.IsRepeated() {
			if rep > 0 {
				(*result)[slicePos] += fmt.Sprintf("[ind[%v]]", rep-1)
			}
			slicePos = len(*result)
			rep++
		}
		*result = append(*result, node.Field.Name)
	}
}

/*
	"A{}",
	"A{B: &B{}}",
	"A{B: &B{C: &C{}}}",
	"A{B: &B{C: &C{D: &D{}}}}"
*/

//DefCaseAppendPath produces case init value
func (n Nodes) DefCaseValue(def int) string {

	node := n.Leaf()
	maxDef := n.MaxDef()
	maxRep := n.MaxRep()

	if maxDef == def {
		if maxRep == 0 {
			result := "aVal"
			if n.Leaf().Field.IsPointer {
				result = fmt.Sprintf("&%s", result)
			}
			switch n.MaxDef() {
			case 1:
				return fmt.Sprintf("%v", result)
			default:
				min := n.DefCasePathPos(def)
				for i := node.Pos - 1; i >= min; i-- {
					idx := i
					childIdx := i + 1
					result = allocNodeSnippet(n[idx].Field, n[childIdx].Field, result, false)
				}
				return result
			}
		}
	}

	result := ""
	pos := n.DefCasePathPos(def)
	if def == 1 {
		min := n.DefCasePathPos(def)
		append := def == 1
		node := n[min]
		result = allocLeafSnippet(node.Field, append)
	} else {
		for i := def - 1; i >= pos; i-- {
			isLeaf := i+1 == def
			node := n[i]
			if isLeaf {
				append := def == 1
				result = allocLeafSnippet(node.Field, append)
			} else {
				append := node.IsRepeated() && i == pos
				result = allocNodeSnippet(node.Field, n[i+1].Field, result, append)
			}
		}
	}
	return result
}

//DefCaseAppendPath produces accessorOpt to append at depth
func (n Nodes) RepCasePath(rep int) string {
	isRootLevel := true
	if rep > 0 {
		isRootLevel = false
		rep--
	}
	pos := n.RepPos(rep)
	if isRootLevel {
		pos = 0
	}
	var result = []string{"v"}
	n.nodePath(pos, &result)
	return strings.Join(result, ".")
}

/*
	case 0:
				x.A = []A{{B: &B{C: &C{D: &D{S: []string{vals[nVals]}}}}}}
			case 1:
				x.A = append(x.A, A{B: &B{C: &C{D: &D{S: []string{vals[nVals]}}}}})
			case 2:
				x.A[ind[0]].B.C.D.S = append(x.A[ind[0]].B.C.D.S, vals[nVals])

*/

//RepCaseValue produces accessorOpt to append at depth
func (n Nodes) RepCaseValue(rep int) string {
	isRootLevel := true
	if rep > 0 {
		isRootLevel = false
		rep--
	}
	rootAppend := !isRootLevel
	result := "aVal"
	leafNode := n[len(n)-1]
	pos := n.RepPos(rep)
	if isRootLevel {
		pos = 0
	}


	if leafNode.IsRepeated() && (pos != leafNode.Pos || isRootLevel) {
		result = fmt.Sprintf("[]%v{%v}", leafNode.Field.ComponentType, result)
	}


	for i := len(n) - 2; i >= pos; i-- {
		node := n[i]
		append := !node.IsRepeated()
		if i == pos {
			append = rootAppend
		}
		result = allocNodeSnippet(node.Field, n[i+1].Field, result, append)
	}
	return result
}

func (n Nodes) Leaf() *Node {
	return n[len(n)-1]
}

func (n Nodes) RepetitionTypes() []int {
	var result = make([]int, len(n))
	for i, item := range n {
		if item.IsRepeated() {
			result[i] = int(parquet.Repeated)
			continue
		}
		if item.IsOptional() {
			result[i] = int(parquet.Optional)
		}

	}
	return result
}

func (n *Nodes) Init(omitEmpty bool)  {
	rep := 0
	def := 0
	for i, item := range *n {
		if omitEmpty {
			(*n)[i].optional = true
		}
		if item.IsRepeated() {
			rep++
			(*n)[i].Rep = rep
		}
		if item.IsOptional() {
			def++
			(*n)[i].Def = def
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
		if node.IsRepeated() {
			elements = []string{}
			//			elements = append(elements, fmt.Sprintf("v%v", node.Pos))
			continue
		}
		elements = append(elements, node.Field.Name)
	}
	return strings.Join(elements, ".")
}

func (n Nodes) typeModifier(nod *Node) string {
	if nod.Field.IsPointer {
		if nod.IsRepeated() {
			return "*"
		} else {
			return "&"
		}
	}
	return ""
}



func (n Nodes) SchemaOptions() string {
	return n.Leaf().schemaOptions
}

func (n Nodes) NewParams(code string) *NodeParams {
	leaf := n.Leaf()
	return &NodeParams{
		OwnerType:        n[0].OwnerType,
		FieldName:        leaf.Field.Name,
		FieldType:        leaf.Field.TypeName,
		ParquetType:      lookupParquetType(leaf.Field.TypeName),
		MethodSuffix:     n.MethodSuffix(),
		Code:             code,
		MaxRep:           n.MaxRep(),
		CastNativeBegin:  leaf.CastNativeBegin(),
		CastNativeEnd:    leaf.CastNativeEnd(),
		CastParquetEnd:   leaf.CastParquetEnd(),
		CastParquetBegin: leaf.CastParquetBegin(),
		StructType:       leaf.StructType(n.MaxDef()),
		OwnerPath:        leaf.OwnerPath,
		OwnerAlias:       strings.ToLower(n[0].OwnerType[0:1]),
		ParquetTypeTitle: strings.Title(lookupParquetType(leaf.Field.TypeName)),
	}
}

type NodeParams struct {
	MethodSuffix     string
	OwnerType        string
	ParquetType      string
	ParquetTypeTitle string
	FieldName        string
	OwnerPath        string
	FieldPath        string
	FieldType        string
	Code             string
	CastNativeBegin  string
	CastNativeEnd    string
	CastParquetBegin string
	CastParquetEnd   string
	StructType       string
	OwnerAlias       string
	MaxRep           int
}
