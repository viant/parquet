package codegen

import (
	"context"
	"fmt"
	"github.com/stretchr/testify/assert"
	"github.com/viant/afs"
	"github.com/viant/toolbox"
	"os"
	"path"
	"testing"
)

func TestGen(t *testing.T) {

	parent := path.Join(toolbox.CallerDirectory(3), "testdata")
	os.Setenv("AST_DEBUG", "0")
	fs :=afs.New()
	var useCases = []struct {
		description string
		options     *Options
		hasError    bool
	}{

		//{
		//	description: "primitive  types",
		//	options: &Options{
		//		Source: path.Join(parent, "primitives"),
		//		Type:   "Message",
		//		Dest:   path.Join(parent, "primitives", "message_enc.go"),
		//	},
		//},
		//
		//{
		//	description: "filter  types",
		//	options: &Options{
		//		Source: path.Join(parent, "filter"),
		//		Type:   "Selection",
		//		Dest:   path.Join(parent, "filter", "selection_enc.go.1"),
		//	},
		//},
		{
			description: "nested  types",
			options: &Options{
				Source: path.Join(parent, "nested"),
				Type:   "Message",
				Dest:   path.Join(parent, "nested", "message_enc.go"),
			},
		},
	}

	for _, useCase := range useCases[0:1] {
		fs.Delete(context.Background(), useCase.options.Dest)
		err := Generate(useCase.options)
		if ! assert.Nil(t, err, useCase.hasError) {
			fmt.Printf("%v\n", err)
		}

	}
}
