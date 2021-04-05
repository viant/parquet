package codegen

import (
	"bytes"
	_ "embed"
	"fmt"
	"text/template"
)

const (
	primitiveRequiredFieldType = iota
	primitiveOptionalFieldType
	primitiveSliceFieldType
	primitiveType
	primitiveOptionalType
	primitiveSliceType
	optionalStringType
	requiredStringType
	mainType
)

//go:embed tmpl/main.tmpl
var mainTmpl string

//go:embed tmpl/field_required.tmpl
var FieldRequiredTmpl string

//go:embed tmpl/field_optional.tmpl
var FieldOptionalTmpl string

//go:embed tmpl/field_slice.tmpl
var FieldSliceTmpl string

//go:embed tmpl/primitive.tmpl
var primitiveTypeTmpl string

//go:embed tmpl/primitive_opt.tmpl
var primitiveTypeOptionalTmpl string

//go:embed tmpl/primitive_slice.tmpl
var primitiveTypeSliceTmpl string

//go:embed tmpl/field_optional_string.tmpl
var fieldOptionalStringTmpl string

//go:embed tmpl/field_required_string.tmpl
var fieldRequiredStringTmpl string

var amTemplate = map[int]string{
	primitiveType:         primitiveTypeTmpl,
	primitiveOptionalType: primitiveTypeOptionalTmpl,
	primitiveSliceType:    primitiveTypeSliceTmpl,
}

var mainTemplate = map[int]string{
	mainType: mainTmpl,
}

var fieldTypeTemplate = map[int]string{
	primitiveRequiredFieldType: FieldRequiredTmpl,
	primitiveOptionalFieldType: FieldOptionalTmpl,
	primitiveSliceFieldType:    FieldSliceTmpl,

	optionalStringType: fieldOptionalStringTmpl,
	requiredStringType: fieldRequiredStringTmpl,
}

//expandTemplate replaces templates parameters with actual data
func expandTemplate(namespace string, dictionary map[int]string, key int, data interface{}) (string, error) {
	var id = fmt.Sprintf("%v_%v", namespace, key)
	textTemplate, ok := dictionary[key]
	if !ok {
		return "", fmt.Errorf("failed to lookup template for %v.%v", namespace, key)
	}
	aTemplate, err := template.New(id).Parse(textTemplate)
	if err != nil {
		return "", fmt.Errorf("fiailed to parse template %v %v, due to %v", namespace, key, err)
	}
	writer := new(bytes.Buffer)
	err = aTemplate.Execute(writer, data)
	return writer.String(), err
}

func expandAccessorMutatorTemlate(key int, data interface{}) (string, error) {
	return expandTemplate("am", amTemplate, key, data)
}

func expandFieldTemplate(key int, data interface{}) (string, error) {
	return expandTemplate("fieldTypeTemplate", fieldTypeTemplate, key, data)
}

func expandMainTemplate(key int, data interface{}) (string, error) {
	return expandTemplate("main", mainTemplate, key, data)
}
