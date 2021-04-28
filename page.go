package parquet

import (
	"fmt"
	"github.com/apache/thrift/lib/go/thrift"
	sch "github.com/viant/parquet/schema"
	"io"
)

// Page keeps track of metadata for each ColumnChunk
type Page struct {
	// N is the number of values in the ColumnChunk
	N      int
	Size   int
	Offset int64
	Codec  sch.CompressionCodec
}


// PageHeader reads the page header from a column page
func PageHeader(r io.Reader) (*sch.PageHeader, error) {
	p := thrift.NewTCompactProtocol(&thrift.StreamTransport{Reader: r})
	pg := &sch.PageHeader{}
	err := pg.Read(p)
	return pg, err
}

// PageHeaders reads all the page headers without reading the actual
// data.  It is used by parquetgen to print the page headers.
func PageHeaders(footer *sch.FileMetaData, r io.ReadSeeker) ([]sch.PageHeader, error) {
	var pageHeaders []sch.PageHeader
	for _, rg := range footer.RowGroups {
		for _, col := range rg.Columns {
			h, err := PageHeadersAtOffset(r, col.MetaData.DataPageOffset, col.MetaData.NumValues)
			if err != nil {
				return nil, err
			}
			pageHeaders = append(pageHeaders, h...)
		}
	}
	return pageHeaders, nil
}

// PageHeadersAtOffset seeks to the given offset, then reads the PageHeader
// without reading the data.
func PageHeadersAtOffset(r io.ReadSeeker, o, n int64) ([]sch.PageHeader, error) {
	var out []sch.PageHeader
	var nRead int64
	_, err := r.Seek(o, io.SeekStart)
	if err != nil {
		return nil, fmt.Errorf("unable to seek to offset %d, err: %s", o, err)
	}

	var readOne bool
	if n > 0 {
		readOne = true
	}

	for !readOne || nRead < n {
		if n == 0 {
			readOne = true
		}
		rc := &readCounter{r: r}
		ph, err := PageHeader(rc)
		if err != nil {
			return nil, fmt.Errorf("unable to read page header: %s", err)
		}
		out = append(out, *ph)
		_, err = r.Seek(int64(ph.CompressedPageSize), io.SeekCurrent)
		if err != nil {
			return nil, fmt.Errorf("unable to seek to next page: %s", err)
		}

		nRead += int64(ph.DataPageHeader.NumValues)
	}
	return out, nil
}
