package codegen

import "github.com/viant/toolbox"

func generateSliceAccessor(sess *session, field *toolbox.FieldInfo, predecessor []string, accessorCode *[]string, rootType string) {
	depthLevel(sess, append(predecessor, field.Name), rootType)
}


func depthLevel(sess *session, path []string, rootType string) int {
	aType := sess.FileSetInfo.Type(rootType)
	aField := aType.Field(path[0])
	depth := 0
	if aField.IsPointer || aField.IsSlice || sess.Options.OmitEmpty  { //TODO || annotation omitempty
		depth++
	}
	path = path[1:]
	if len(path) == 0 {
		return depth
	}
	return depth + depthLevel(sess, path, aField.TypeName)
}