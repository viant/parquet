package filter

import (
	"bufio"
	"context"
	"fmt"
	"github.com/francoispqt/gojay"
	"github.com/viant/afs"
	"log"
	"os"
)

func Transcode(ctx context.Context, location string, dest string) error{
	fs := afs.New()
	reader, err := fs.OpenURL(ctx, location)
	if err != nil {
		return err
	}
	defer reader.Close()
	f , err := os.Create(dest)
	defer f.Close()
	if err != nil {
		fmt.Errorf("error creating a file %v", err)
	}
	fw, err := NewParquetWriter(f, MaxPageSize(1000), Gzip)
	if err != nil {
		log.Fatal(err)
	}
	aScanner := bufio.NewScanner(reader)
	if err != nil {
		log.Fatal(err)
	}
	for aScanner.Scan() {
		data := aScanner.Bytes()
		selection := &Selection{}
		if err = gojay.Unmarshal(data, selection);err != nil {
			continue
			//return fmt.Errorf("failed to unmarshall: %w, data: '%s'",err, data)
		}
		fw.Add(*selection)
	}
	if err := fw.Write(); err != nil {
		log.Fatal(err)
	}

	if err := fw.Close(); err != nil {
		log.Fatal(err)
	}
	return nil
}







