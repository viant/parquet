package codegen

import (
	"github.com/stretchr/testify/assert"
	"github.com/viant/toolbox"
	"os"
	"path"
	"testing"
)

func TestGen(t *testing.T) {

	parent := path.Join(toolbox.CallerDirectory(3), "testdata")
	os.Setenv("AST_DEBUG", "0")
	var useCases = []struct {
		description string
		options     *Options
		hasError    bool
	}{

		{
			description: "primitive  types",
			options: &Options{
				Source: path.Join(parent, "primitives"),
				Type:   "Message",
				Dest:   path.Join(parent, "primitives", "message_enc.go"),
			},
		},
	}

	for _, useCase := range useCases[0:1] {
		err := Generate(useCase.options)
		assert.Nil(t, err, useCase.hasError)

	}
}
