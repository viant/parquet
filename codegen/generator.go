package codegen

import (
	"fmt"
	"github.com/viant/toolbox"
	"io/ioutil"
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
	//	compressionFields := make([]string,0)
	if err := generateStructCoding(session, []string{}, options.Type); err != nil {
		return err
	}

	prefix, err := expandMainTemplate(mainType, struct {
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

	dest := session.Dest
	//	err = ioutil.WriteFile(dest, []byte(prefix+strings.Join(session.fieldStructCode, "")), 0644)
	err = ioutil.WriteFile(dest, []byte(prefix), 0644)
	session.fieldStructCode = []string{}
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

func generateStructCoding(sess *session, path []string, typeName string) error {
	if ok := sess.shallGenerateCode(typeName); !ok {
		return nil
	}
	typeInfo := sess.FileSetInfo.Type(typeName)

	for _, field := range typeInfo.Fields() {
		fmt.Printf("field into :: %v\n", field)
		err := generateFieldCode(sess, []string{}, field, typeName)
		if err != nil {
			return err
		}

	}
	return nil
}

func generateFieldCode(sess *session, predecessor []string, field *toolbox.FieldInfo, rootType string) error {
	ownerAlias := strings.ToLower(rootType[0:1])
	isLeafNode := isBaseType(field.TypeName)
	if isLeafNode {
		if err := generateAM(sess, predecessor, field, rootType); err != nil {
			return err
		}
		generateFieldInits(sess, predecessor, field)
	}
	params := NewFieldParams(rootType, ownerAlias, field.Name, field.TypeName, field.ComponentType)
	if !sess.shallGenerateParquetFieldType(params.ParquetType, field) {
		return nil
	}

	var err error
	var code string
	if !isLeafNode {
		aType := sess.FileSetInfo.Type(field.TypeName)
		if aType.IsStruct {
			path := append(predecessor, field.Name)
			for _, field := range aType.Fields() {
				if err = generateFieldCode(sess, path, field, rootType); err != nil {
					return err
				}
			}

		} else if aType.IsSlice {

		}
		return nil
	}

	if sess.OmitEmpty || field.IsPointer || field.IsSlice || isLeafNode {
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



func generateAM(sess *session, predecessor []string, field *toolbox.FieldInfo, rootType string) error {
	ownerAlias := strings.ToLower(rootType[0:1])
	params := NewFieldParams(rootType, ownerAlias, field.Name, field.TypeName, "")
	var code string
	var err error
	if (sess.Options.OmitEmpty || field.IsSlice || field.IsPointer || isBaseType(field.TypeName)) && field.ComponentType != "string" {
		if field.IsSlice {
			//TODO slice logic
		}
		if field.IsPointer {
			code, err = expandAccessorMutatorTemlate(primitiveOptionalType, params)
			if err != nil {
				return err
			}
			sess.addAccessorMutatorSnippet(code)
			return nil

		} else {
			code, err = expandAccessorMutatorTemlate(primitiveType, params)
			if err != nil {
				return err
			}
			sess.addAccessorMutatorSnippet(code)
			return nil
		}

	}
	if field.TypeName == "string" || field.ComponentType == "string" {
		code, err = expandAccessorMutatorTemlate(primitiveType, params)
		if err != nil {
			return err
		}
		sess.addAccessorMutatorSnippet(code)
		return nil
	}
	return nil
}

func generateFieldInits(sess *session, predecessor []string, field *toolbox.FieldInfo) {
	var code string
	if sess.Options.OmitEmpty || field.IsPointer || field.IsSlice {
		code = getOptionalFieldInit(field)
	} else {
		code = getRequiredFieldInit(predecessor, field)
	}
	sess.addFieldInitSnippet(code)
}

//isBaseType checks if typeName is primitive types
func isBaseType(typeName string) bool {
	switch typeName {
	case "bool", "int", "int8", "int16", "int32", "int64", "uint", "uint8", "uint16", "uint32", "uint64", "float32", "float64", "string", "[]string",
		"[]int", "[]int32", "[]int64":
		return true
	}
	return false
}
