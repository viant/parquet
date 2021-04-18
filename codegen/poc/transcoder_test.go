package poc

import (
	"context"
	"fmt"
	"github.com/stretchr/testify/assert"
	"github.com/viant/toolbox"
	"path"
	"testing"
	"time"
)

func TestTranscode(t *testing.T) {

	director := toolbox.CallerDirectory(3)

	var useCases = []struct {
		description string
		location    string
		destination string
	}{
		{
			description: "small  file with snappy codec",
			location:    path.Join(director, "test_data/selection_3.log"),
			destination: path.Join(director, "test_data/selection_snappy_3.parquet"),
		},
	}


	for _, useCase := range useCases[0:1] {
		start := time.Now()
		err := Transcode(context.Background(), useCase.location, useCase.destination)
		assert.Nil(t, err, useCase.description)
		elapsed := time.Now().Sub(start)
		fmt.Printf("elapsed [%v]: %s\n", useCase.destination, elapsed)
	}

}
