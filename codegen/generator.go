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
		AccessorCode:  strings.Join(session.AccessorCode, "\n"),
		FieldTypeCode: strings.Join(session.structCodingCode, "\n"),
		FieldInit:     strings.Join(session.FieldsInit, "\n"),
		OwnerType:     options.Type,
	})
	if err != nil {
		fmt.Printf("failed to generate %v\n", err)
		return err
	}

	dest := session.Dest
	//	err = ioutil.WriteFile(dest, []byte(prefix+strings.Join(session.structCodingCode, "")), 0644)
	err = ioutil.WriteFile(dest, []byte(prefix), 0644)
	session.structCodingCode = []string{}
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
	codings := make([]string, 0)
	accessorCode := make([]string, 0)
	fieldInits := make([]string, 0)
	for _, field := range typeInfo.Fields() {
		fmt.Printf("field into :: %v\n", field)
		receiverAlias := strings.ToLower(typeName[0:1])
			_, err := generateTypeStruct(sess, field, receiverAlias, &codings, typeName)
			if err != nil {
				return err
			}
			_, err = generateAM(sess, field, receiverAlias, &accessorCode, typeName)
			if err != nil {
				return err
			}
			generateFieldInits(sess, []string{}, field, &fieldInits)
	}
	sess.AccessorCode = accessorCode
	sess.FieldsInit = fieldInits
	sess.structCodingCode = codings
	return nil
}

func generateTypeStruct(sess *session, field *toolbox.FieldInfo, ownerAlias string, codings *[]string, ownerType string) (bool, error) {

	params := NewFieldParams(ownerType, ownerAlias, field.Name, field.TypeName, field.ComponentType)
	if !sess.shallGenerateParquetFieldType(params.ParquetType, field) {
		return false, nil
	}

	var err error
	var code string
	if sess.OmitEmpty || field.IsPointer || field.IsSlice || isPrimitiveType(field.TypeName) {
		if field.TypeName == "string" || field.ComponentType == "string"{
			code, err = expandFieldTemplate(optionalStringType, params)
			*codings = append(*codings, code)
			if err != nil {
				return false, err
			}
			return true, nil
		}
		if field.IsSlice {
			code, err = expandFieldTemplate(primitiveSliceFieldType, params)
			*codings = append(*codings, code)
			if err != nil {
				return false, err
			}
			return true, nil
		}

		if field.IsPointer { // || or filed omit empty annotation
			code, err = expandFieldTemplate(primitiveOptionalFieldType, params)
			*codings = append(*codings, code)
			if err != nil {
				return false, err
			}
			return true, nil
		} else {
			code, err = expandFieldTemplate(primitiveRequiredFieldType, params)
			*codings = append(*codings, code)
			if err != nil {
				return false, err
			}
			return true, nil
		}
	}
	if field.TypeName == "string" {
		code, err = expandFieldTemplate(requiredStringType, params)
		*codings = append(*codings, code)
		if err != nil {
			return false, err
		}
		return true, nil
	}

	return true, nil
}

func generateAM(sess *session, field *toolbox.FieldInfo, alias string, accessorCode *[]string, ownerType string) (bool, error) {

	params := NewFieldParams(ownerType, alias, field.Name, field.TypeName, "")
	var code string
	var err error
	if (sess.Options.OmitEmpty || field.IsSlice || field.IsPointer || isPrimitiveType(field.TypeName)) && field.ComponentType != "string"{
		if field.IsSlice {
			code, err = expandAccessorMutatorTemlate(primitiveSliceType, params)
			if err != nil {
				return false, err
			}
			*accessorCode = append(*accessorCode, code)
			return true, nil
		}
		if field.IsPointer {
			code, err = expandAccessorMutatorTemlate(primitiveOptionalType, params)
			if err != nil {
				return false, err
			}
			*accessorCode = append(*accessorCode, code)
			return true, nil

		} else {
			code, err = expandAccessorMutatorTemlate(primitiveType, params)
			if err != nil {
				return false, err
			}
			*accessorCode = append(*accessorCode, code)
			return true, nil

		}

	}
	if field.TypeName == "string" || field.ComponentType == "string"{
		code, err = expandAccessorMutatorTemlate(primitiveType, params)
		if err != nil {
			return false, err
		}
		*accessorCode = append(*accessorCode, code)
		return true, nil
	}

	return true, nil

}

func generateFieldInits(sess *session, fieldPath []string, field *toolbox.FieldInfo, fieldInits *[]string) {
	var code string
	if sess.Options.OmitEmpty || field.IsPointer || field.IsSlice {
		code = getOptionalFieldInit(field)
	} else {
		code = getRequiredFieldInit(fieldPath, field)
	}
	*fieldInits = append(*fieldInits, code)
}

//isPrimitiveType checks if typeName is primitive types
func isPrimitiveType(typeName string) bool {
	switch typeName {
	case  "bool", "int", "int8", "int16", "int32", "int64", "uint", "uint8", "uint16", "uint32", "uint64", "float32", "float64", "time.Time",
		"[]int", "[]int32", "[]int64":
		return true
	}
	return false
}
