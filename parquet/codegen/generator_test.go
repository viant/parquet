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

		{
			description: "required/optional",
			options: &Options{
				Source: path.Join(parent, "base"),
				Type:   "Message",
				Dest:   path.Join(parent, "base", "message_enc.go"),
			},
		},
		{
			description: "repeated",
			options: &Options{
				Source: path.Join(parent, "repeated"),
				Type:   "Message",
				Dest:   path.Join(parent, "repeated", "message_enc.go"),
			},
		},

		{
			description: "optional",
			options: &Options{
				Source: path.Join(parent, "optional"),
				Type:   "Message",
				Dest:   path.Join(parent, "optional", "message_enc.go"),
			},
		},
	}

	for _, useCase := range useCases {
		fs.Delete(context.Background(), useCase.options.Dest)
		err := Generate(useCase.options)
		if ! assert.Nil(t, err, useCase.hasError) {
			fmt.Printf("%v\n", err)
		}

	}
}
