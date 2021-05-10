package codegen

import (
	"context"
	_ "embed"
	"fmt"
	"github.com/viant/afs"
	"github.com/viant/afs/file"
	"go/format"
	"strings"
)

//go:embed tmpl/main.tmpl
var mainTmpl Template

//Generate generates transformed code into  output file
func Generate(options *Options) error {
	session := newSession(options)
	addRequiredImports(session)
	err := session.readPackageCode()
	if err != nil {
		return err
	}
	if err := generatePathCode(session, Nodes{}, options.Type); err != nil {
		return err
	}

	param := struct {
		Pkg           string
		Imports       string
		AccessorCode  string
		FieldTypeCode string
		FieldInit     string
		OwnerType     string
	}{
		Pkg:           session.Pkg,
		Imports:       session.getImports(),
		AccessorCode:  strings.Join(session.accessorMutatorCode, "\n"),
		FieldTypeCode: strings.Join(session.fieldStructCode, "\n"),
		FieldInit:     strings.Join(session.fieldInitCode, "\n"),
		OwnerType:     options.Type,
	}
	code, err := mainTmpl.Expand("main", param)
	if err != nil {
		fmt.Printf("failed to generate %v\n", err)
		return err
	}
	//dest := session.Dest
	fs := afs.New()
	formatted, err := format.Source([]byte(code))
	if err == nil {
		code = string(formatted)
	}
	err = fs.Upload(context.Background(), session.Dest, file.DefaultFileOsMode, strings.NewReader(code))
	return err
}

func addRequiredImports(session *session) {
	session.addImport("io")
	session.addImport("strings")
	session.addImport("fmt")
	session.addImport("github.com/viant/parquet")
}

func generatePathCode(sess *session, nodes Nodes, typeName string) error {
	typeInfo := sess.FileSetInfo.Type(normalizeTypeName(typeName))
	if typeInfo == nil {
		return fmt.Errorf("failed to lookup type %v", typeName)
	}
	fields := typeInfo.Fields()
	for i, field := range fields {
		normalizedType := normalizeTypeName(field.TypeName)
		if idx := strings.LastIndex(normalizedType, "."); idx != -1 {
			sess.addImport(normalizedType[:idx])
		}
		node := NewNode(sess, typeName, fields[i])
		if strings.Contains(node.Field.TypeName, "Time") {
			sess.addImport("time")
		}
		fieldNodes := append(nodes, node)
		if isBaseType(field.TypeName) {
			fieldNodes.Init(sess.OmitEmpty)
			err := generateFieldCode(sess, fieldNodes)
			if err != nil {
				return err
			}
			continue
		}
		if err := generatePathCode(sess, fieldNodes, field.TypeName); err != nil {
			return err
		}
	}
	return nil
}

func generateFieldCode(sess *session, nodes Nodes) error {
	if err := generateAccessor(sess, nodes); err != nil {
		return err
	}
	if err := generateMutator(sess, nodes); err != nil {
		return err
	}
	generateFieldInits(sess, nodes)
	params := nodes.NewParams("")

	if !sess.shallGenerateParquetFieldType(params.StructType) {
		return nil
	}

	if nodes.MaxDef() == 0 {
		return generateRequiredFieldStruct(sess, nodes)
	}
	return generateOptionalFieldStruct(sess, nodes)
}

func generateFieldInits(sess *session, path Nodes) {
	var code string
	if path.MaxDef() > 0 {
		code = getOptionalFieldInit(path)
	} else {
		code = getRequiredFieldInit(path)
	}
	sess.addFieldInitSnippet(code)
}

//isBaseType checks if typeName is primitive types
func isBaseType(typeName string) bool {
	switch typeName {
	case "bool", "int", "int8", "int16", "int32", "int64", "uint", "uint8", "uint16", "uint32", "uint64", "float32", "float64", "string", "[]string",
		"[]uint", "[]int", "[]int32", "[]int64", "[]uint32", "[]uint64", "[]float64", "[]float32", "[]byte", "[]bool", "time.Time", "*time.Time", "time.StringTime", "time.StringDate":
		return true
	}
	return false
}
