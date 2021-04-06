package codegen

func generateAccessor(sess *session, nodes Nodes) error {

	leaf := nodes.Leaf()
	var childSnippet = ""
	var err error
	var leafParams *FieldParams
	if leaf.Field.IsSlice {
		leafParams = leaf.NewParams()
		leafParams.SetIndent(4 * leaf.Indent())
		leafSnippet, err := expandAccessorMutatorTemlate(sliceReadLeaf, leafParams)
		if err != nil {
			return err
		}
		childSnippet = leafSnippet
	}

	for i := len(nodes) - 2; i >= 0; i-- {
		node := nodes[i]
		params := node.NewParams()
		params.SetIndent(4 * node.Indent())
		params.ChildSnippet = childSnippet
		if node.Field.IsSlice {
			if childSnippet, err = expandAccessorMutatorTemlate(sliceReadNode, params); err != nil {
				return err
			}
		}
	}


	root := nodes[0]
	rootParams := root.NewParams()
	rootParams.MethodSuffix = nodes.MethodSuffix()
	rootParams.ParquetType = leafParams.ParquetType
	rootParams.ChildSnippet = childSnippet
	code, err := expandAccessorMutatorTemlate(accessorRoot, rootParams)
	if err != nil {
		return err
	}
	sess.addAccessorMutatorSnippet(code)
	//fmt.Printf("%v, %v\n", code, err)

	return err
}
