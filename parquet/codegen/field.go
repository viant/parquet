package codegen

import _ "embed"

//go:embed tmpl/field_required.tmpl
var fieldRequiredStructTmpl Template

//go:embed tmpl/field_optional.tmpl
var fieldOptionalStructTmpl Template

//go:embed tmpl/field_required_string.tmpl
var fieldRequiredStringStructTmpl Template

//go:embed tmpl/field_optional_string.tmpl
var fieldOptionalStringStructTmpl Template

//go:embed tmpl/field_optional_bool.tmpl
var fieldOptionalBoolStructTmpl Template

//go:embed tmpl/field_required_bool.tmpl
var fieldRequiredBoolStructTmpl Template


func generateRequiredFieldStruct(sess *session, nodes Nodes) error {
	params := nodes.NewParams("")

	if normalizeTypeName(nodes.Leaf().Field.TypeName) == "bool" {
		code, err := fieldRequiredBoolStructTmpl.Expand("fieldRequiredStringStruct", params)
		if err != nil {
			return err
		}
		sess.addFieldStructSnippet(code)
		return nil
	}
	sess.addImport("bytes")
	sess.addImport("encoding/binary")
	if lookupParquetType(nodes.Leaf().Field.TypeName) == "string" {
		sess.addImport("sort")
		code, err := fieldRequiredStringStructTmpl.Expand("fieldRequiredStringStruct", params)
		if err != nil {
			return err
		}
		sess.addFieldStructSnippet(code)
		return nil
	}

	addNumericImports(sess)
	code, err := fieldRequiredStructTmpl.Expand("fieldRequired", params)
	if err != nil {
		return err
	}
	sess.addFieldStructSnippet(code)
	return generateRequiredFieldStatStruct(sess, nodes)
}

func generateOptionalFieldStruct(sess *session, nodes Nodes) error {
	params := nodes.NewParams("")

	if normalizeTypeName(nodes.Leaf().Field.TypeName) == "bool" {
		code, err := fieldOptionalBoolStructTmpl.Expand("fieldOptionalStringStructTmpl", params)
		if err != nil {
			return err
		}
		sess.addFieldStructSnippet(code)
		return nil
	}

	sess.addImport("bytes")
	sess.addImport("encoding/binary")
	if normalizeTypeName(nodes.Leaf().Field.TypeName) == "string" || nodes.Leaf().Field.TypeName == "[]byte" {
		sess.addImport("sort")
		code, err := fieldOptionalStringStructTmpl.Expand("fieldOptionalStringStructTmpl", params)
		if err != nil {
			return err
		}
		sess.addFieldStructSnippet(code)
		return nil
	}

	addNumericImports(sess)

	code, err := fieldOptionalStructTmpl.Expand("fieldRequired", params)
	if err != nil {
		return err
	}
	sess.addFieldStructSnippet(code)
	return generateOptionalFieldStatStruct(sess, nodes)
}

func addNumericImports(sess *session) {
	sess.addImport("math")
}
