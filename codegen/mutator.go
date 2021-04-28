package codegen

import (
	_ "embed"
	"fmt"
	"strings"
)

//go:embed tmpl/mutator_rep.tmpl
var mutatorRepeatedTmpl Template

//go:embed tmpl/mutator_opt.tmpl
var mutatorOptionalTmpl Template

//go:embed tmpl/mutator_req.tmpl
var mutatorRequiredTmpl Template

func generateMutator(sess *session, nodes Nodes) error {
	params := nodes.NewParams("")
	params.MethodSuffix = nodes.MethodSuffix()

	switch {
	case nodes.MaxDef() == 0:
		return generateRequiredMutator(sess, nodes)
	case nodes.MaxRep() == 0:
		return generateOptionalMutator(sess, nodes)
	}
	return generateRepeatedMutator(sess, nodes)
}

func generateRepeatedMutator(sess *session, nodes Nodes) error {
	var cases = make([]string, 0)
	for def := 1; def < nodes.MaxDef(); def++ {
		path := nodes.DefCasePath(def)
		value := nodes.DefCaseValue(def)
		cases = append(cases, fmt.Sprintf(`case %v:
%v = append(%v, %v)
`, def, path, path, value))
	}

	var repCases = make([]string, 0)
	for i := 0; i <= nodes.MaxRep(); i++ {
		path := nodes.RepCasePath(i)
		value := nodes.RepCaseValue(i)
		if i == 0 {
			value = fmt.Sprintf(`%v`, value)
		} else {
			value = fmt.Sprintf(`append(%v, %v)`, path, value)

		}
		repCases = append(repCases, fmt.Sprintf(`case %v:
	%s = %s
`, i, path, value))
	}

	cases = append(cases, fmt.Sprintf(`case %v:
	switch rep {
	%v
	}
`, nodes.MaxDef(), strings.Join(repCases, "")))

	code, err := mutatorRepeatedTmpl.Expand("mutatorRepeated", nodes.NewParams(strings.Join(cases, "")))
	if err != nil {
		return err
	}
	sess.addMutatorSnippet(code)
	return nil
}

func generateOptionalMutator(sess *session, nodes Nodes) error {
	def := nodes.MaxDef()
	var cases = make([]string, def)
	for i := 1; i <= def; i++ {
		cases[i-1] = fmt.Sprintf(`case %v:
	%v = %v
`, i, nodes.DefCasePath(i), nodes.DefCaseValue(i),
		)
	}
	code, err := mutatorOptionalTmpl.Expand("mutator_optional", nodes.NewParams(strings.Join(cases, "")))
	if err != nil {
		return err
	}
	sess.addMutatorSnippet(code)
	return nil
}

func generateRequiredMutator(sess *session, nodes Nodes) error {
	code, err := mutatorRequiredTmpl.Expand("mutator_required", nodes.NewParams(""))
	if err != nil {
		return err
	}
	sess.addMutatorSnippet(code)
	return nil
}
