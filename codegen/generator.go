package codegen

import (
	"context"
	"fmt"
	"github.com/viant/afs"
	"github.com/viant/afs/file"
	"strings"
)

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

	code, err := expandMainTemplate(mainType, struct {
		Pkg           string
		Imports       string
		AccessorCode  string
		FieldTypeCode string
		FieldInit     string
		OwnerType     string
	}{
		Pkg:           session.pkg,
		Imports:       session.getImports(),
		AccessorCode:  strings.Join(session.accessorMutatorCode, "\n"),
		FieldTypeCode: strings.Join(session.fieldStructCode, "\n"),
		FieldInit:     strings.Join(session.fieldInitCode, "\n"),
		OwnerType:     options.Type,
	})
	if err != nil {
		fmt.Printf("failed to generate %v\n", err)
		return err
	}

	//dest := session.Dest
	fs := afs.New()
	err = fs.Upload(context.Background(),session.Dest,file.DefaultFileOsMode, strings.NewReader(code+strings.Join(session.fieldStructCode, "")))
	return err
}

func addRequiredImports(session *session) {
	session.addImport("io")
	session.addImport("bytes")
	session.addImport("math")
	session.addImport("strings")
	session.addImport("fmt")
	session.addImport("encoding/binary")
	session.addImport("github.com/viant/parquet")
	session.addImport("sch github.com/viant/parquet/schema")
}

func generatePathCode(sess *session, nodes Nodes, typeName string) error {
	typeInfo := sess.FileSetInfo.Type(normalizeTypeName(typeName))
	if typeInfo == nil {
		return fmt.Errorf("failed to lookup type %v", typeName)
	}
	fields := typeInfo.Fields()
	for i, field := range fields {
		node := NewNode(sess, typeName, fields[i])
		fieldNodes := append(nodes, node)
		if isLeafType(field.TypeName) {
			fieldNodes.Init()
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

	generateFieldInits(sess, nodes)

	node := nodes.Leaf()
	params := node.NewParams()
	field := node.Field
	if !sess.shallGenerateParquetFieldType(params.ParquetType, node.Field) {
		return nil
	}
	var err error
	var code string
	if sess.OmitEmpty || field.IsPointer || field.IsSlice {
		if field.TypeName == "string" || field.ComponentType == "string" {
			code, err = expandFieldTemplate(optionalStringType, params)
			sess.addFieldStructSnippet(code)
			return err
		}
		if field.IsSlice {
			code, err = expandFieldTemplate(primitiveSliceFieldType, params)
			sess.addFieldStructSnippet(code)
			return err
		}

		if field.IsPointer { // || or filed omit empty annotation
			code, err = expandFieldTemplate(primitiveOptionalFieldType, params)
			sess.addFieldStructSnippet(code)
			return err
		} else {
			code, err = expandFieldTemplate(primitiveRequiredFieldType, params)
			sess.addFieldStructSnippet(code)
			return err
		}
	}
	if field.TypeName == "string" {
		code, err = expandFieldTemplate(requiredStringType, params)
		sess.addFieldStructSnippet(code)
		return err
	}
	return nil
}

func generateAM(sess *session, nodes Nodes) error  {
	leaf := nodes.Leaf()
	if leaf.Field.IsSlice {
		if err := generateAccessor(sess, nodes);err != nil {
			return err
		}
	}
	//ownerAlias := strings.ToLower(rootType[0:1])
	//params := NewFieldParams(rootType, ownerAlias, field.Name, field.TypeName, "", 0)
	//var code string
	//var err error
	//if (sess.Options.OmitEmpty || field.IsSlice || field.IsPointer || isLeafType(field.TypeName)) && field.ComponentType != "string" {
	//	if field.IsSlice {
	//		//TODO slice logic
	//	}
	//	if field.IsPointer {
	//		code, err = expandAccessorMutatorTemlate(primitiveOptionalType, params)
	//		if err != nil {
	//			return err
	//		}
	//		sess.addAccessorMutatorSnippet(code)
	//		return nil
	//
	//	} else {
	//		code, err = expandAccessorMutatorTemlate(primitiveType, params)
	//		if err != nil {
	//			return err
	//		}
	//		sess.addAccessorMutatorSnippet(code)
	//		return nil
	//	}
	//
	//}
	//if field.TypeName == "string" || field.ComponentType == "string" {
	//	code, err = expandAccessorMutatorTemlate(primitiveType, params)
	//	if err != nil {
	//		return err
	//	}
	//	sess.addAccessorMutatorSnippet(code)
	//	return nil
	//}
	return nil
}

func generateFieldInits(sess *session, path Nodes) {
	var code string
	node := path.Leaf()
	if node.IsOptional {
		code = getOptionalFieldInit(path)
	} else {
		code = getRequiredFieldInit(path)
	}
	sess.addFieldInitSnippet(code)
}

//isLeafType checks if typeName is primitive types
func isLeafType(typeName string) bool {
	switch typeName {
	case "bool", "int", "int8", "int16", "int32", "int64", "uint", "uint8", "uint16", "uint32", "uint64", "float32", "float64", "string", "[]string",
		"[]int", "[]int32", "[]int64":
		return true
	}
	return false
}
