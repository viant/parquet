package codegen

import (
	_ "embed"
	"fmt"
	"strings"
)

//go:embed tmpl/accessor_req.tmpl
var accessorRequiredTmpl Template

//go:embed tmpl/accessor_rep.tmpl
var accessorRepeatedNode Template

//go:embed tmpl/accessor_opt.tmpl
var accessorRepeatedOptionalTmpl Template

func generateAccessor(sess *session, nodes Nodes) error {

	switch {
	case nodes.MaxDef() == 0:
		return generateRequiredAccessor(sess, nodes)
	case nodes.MaxRep() == 0:
		return generateOptionalAccessor(sess, nodes)
	default:
		return generateRepeatedAccessor(sess, nodes)
	}
}

func generateRequiredAccessor(sess *session, nodes Nodes) error {
	code, err := accessorRequiredTmpl.Expand("accessorRequired", nodes.NewParams(""))
	if err != nil {
		return err
	}
	sess.addAccessorSnippet(code)
	return nil
}

func generateOptionalAccessor(sess *session, nodes Nodes) error {
	var out string
	maxDef := nodes.MaxDef()
	for def := 0; def < maxDef; def++ {
		node := nodes.DefNilNode(def)
		checkValue := node.CheckValue()
		checkBegin := ""
		checkEnd := ""
		if node.Field.TypeName == "[]byte" {
			checkBegin = "len("
			checkEnd = ")"
		}
		out += fmt.Sprintf(`case %s%s%s%s:
		return nil, []uint8{%d}, nil
`, checkBegin, node.Path(), checkEnd, checkValue, def)

	}
	leaf := nodes.Leaf()

	var ptr string
	if leaf.Field.IsPointer {
		ptr = "*"
	}
	out += fmt.Sprintf(`	default:
		return []%s{%s%s%s%s}, []uint8{%d}, nil`, leaf.ParquetType(),
		leaf.CastParquetBegin(),
		ptr,
		leaf.Path(),
		leaf.CastParquetEnd(),
		maxDef)
	node := nodes[0]
	code := fmt.Sprintf(`func read%s(v *%s) ([]%s, []uint8, []uint8) {
	switch {
	%s
	}
}`, nodes.MethodSuffix(), node.OwnerType, leaf.ParquetType(), out)
	sess.addAccessorSnippet(code)
	return nil
}

func generateRepeatedAccessor(sess *session, nodes Nodes) error {
	leaf := nodes.Leaf()
	body, err := generateRepeatedSnippet(nodes, 0, "v")
	if err != nil {
		return err
	}
	code := fmt.Sprintf(`func read%s(v *%s) ([]%s, []uint8, []uint8) {
	var vals []%s
	var defs, reps []uint8
	var lastRep uint8

	%s

	return vals, defs, reps	
}`,
		nodes.MethodSuffix(),
		nodes[0].OwnerType,
		leaf.ParquetType(),
		leaf.ParquetType(),
		body,
	)
	sess.addAccessorSnippet(code)
	return nil
}

func generateRepeatedSnippet(nodes Nodes, def int, varName string) (string, error) {
	node := nodes.DefNilNode(def)
	if def == nodes.MaxDef() {
		if node.Field.IsPointer {
			varName = fmt.Sprintf("*%s", varName)
		}
		if !node.Field.IsSlice {
			varName = strings.Join(append([]string{varName}, nodes.Names()[node.Pos+1:]...), ".")
		}
		return fmt.Sprintf(`defs = append(defs, %d)
	reps = append(reps, lastRep)
	vals = append(vals, %s%s%s)`, def, node.CastParquetBegin(), varName, node.CastParquetEnd()), nil
	}
	nextVar := varName
	param := struct {
		Var        string
		Field      string
		Rep        int
		Def        int
		CheckValue string
	}{
		Var:        varName,
		Field:      node.RelativePath(),
		Rep:        node.Rep,
		Def:        def,
		CheckValue: node.CheckValue(),
	}

	var fragment = ""
	var err error
	if node.Field.IsSlice {
		nextVar = fmt.Sprintf("x%d", node.Rep)
		fragment, err = accessorRepeatedNode.Expand("repeatedNode", param)
		if err != nil {
			return "", err
		}
	} else {
		fragment, err = accessorRepeatedOptionalTmpl.Expand("repeatedOptional", param)
		if err != nil {
			return "", err
		}
	}

	body, err := generateRepeatedSnippet(nodes, def+1, nextVar)
	return fmt.Sprintf(fragment, body), err
}
