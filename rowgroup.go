package parquet

import (
	sch "github.com/viant/parquet/schema"
	"strings"
)

// RowGroup wraps schema.RowGroup and adds accounting functions
// that are used to keep track of number of rows written, byte size,
// etc.
type RowGroup struct {
	fields   schema
	rowGroup sch.RowGroup
	columns  map[string]sch.ColumnChunk
	child    *RowGroup

	Rows int64
}

// Columns returns the Columns of the row group.
func (r *RowGroup) Columns() []*sch.ColumnChunk {
	return r.rowGroup.Columns
}

func (r *RowGroup) updateColumnChunk(pth []string, dataLen, compressedLen, count int, fields schema, comp sch.CompressionCodec) error {
	col := strings.Join(pth, ".")

	ch, ok := r.columns[col]
	if !ok {
		t, err := columnType(col, fields)
		if err != nil {
			return err
		}

		ch = sch.ColumnChunk{
			MetaData: &sch.ColumnMetaData{
				Type:         t,
				Encodings:    []sch.Encoding{sch.Encoding_PLAIN},
				PathInSchema: pth,
				Codec:        comp,
			},
		}
	}

	ch.MetaData.NumValues += int64(count)
	ch.MetaData.TotalUncompressedSize += int64(dataLen)
	ch.MetaData.TotalCompressedSize += int64(compressedLen)
	r.columns[col] = ch
	return nil
}
