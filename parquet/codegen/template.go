package codegen

import (
	"bytes"
	_ "embed"
	"fmt"
	"text/template"
)

//var tmap = template.FuncMap{
//	"inc": func(i int) int {
//		return i + 1
//	},
//}

type Template string

func (t Template) Expand(id string, data interface{}) (string, error) {
	aTemplate, err := template.New(id).Parse(string(t))
	if err != nil {
		panic(fmt.Sprintf("failed to parse template %v, due to %v", id, err))
	}
	writer := new(bytes.Buffer)
	err = aTemplate.Execute(writer, data)
	return writer.String(), err
}
