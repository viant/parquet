package parquet

import (
	"context"
	"encoding/binary"
	"fmt"
	"github.com/apache/thrift/lib/go/thrift"
	sch "github.com/viant/parquet/schema"
	"io"
	"strings"
)

// Metadata keeps track of the things that need to
// be kept track of in order to write the FileMetaData
// at the end of the parquet file.
type Metadata struct {
	ts           *thrift.TSerializer
	schema       schema
	docs         int64
	pageDocs     int64
	rowGroupDocs int64
	rowGroups    []RowGroup
	metadata     *sch.FileMetaData
}

// StartRowGroup is called when starting a new row group
func (m *Metadata) StartRowGroup(fields ...Field) {
	m.rowGroupDocs = 0
	m.rowGroups = append(m.rowGroups, RowGroup{
		fields:  schemaElements(fields),
		columns: make(map[string]sch.ColumnChunk),
	})
}

// NextDoc keeps track of how many documents have been
// added to this parquet file.  The final value of m.docs
// is used for the FileMetaData.NumRows
func (m *Metadata) NextDoc() {
	m.docs++
	m.rowGroupDocs++
	m.pageDocs++
}

// RowGroups returns a summary of each schema.RowGroup
func (m *Metadata) RowGroups() []RowGroup {
	rgs := make([]RowGroup, len(m.metadata.RowGroups))
	for i, rg := range m.metadata.RowGroups {
		rgs[i] = RowGroup{
			rowGroup: *rg,
			Rows:     rg.NumRows,
		}
	}
	return rgs
}


// WritePageHeader is called in order to finish writing to a column chunk.
func (m *Metadata) WritePageHeader(w io.Writer, pth []string, dataLen, compressedLen, defCount, count int, defLen, repLen int64, comp sch.CompressionCodec, stats Stats) error {
	ph := &sch.PageHeader{
		Type:                 sch.PageType_DATA_PAGE,
		UncompressedPageSize: int32(dataLen),
		CompressedPageSize:   int32(compressedLen),
		DataPageHeader: &sch.DataPageHeader{
			NumValues:               int32(count),
			Encoding:                sch.Encoding_PLAIN,
			DefinitionLevelEncoding: sch.Encoding_RLE,
			RepetitionLevelEncoding: sch.Encoding_RLE,
			Statistics: &sch.Statistics{
				NullCount:     stats.NullCount(),
				DistinctCount: stats.DistinctCount(),
				MinValue:      stats.Min(),
				MaxValue:      stats.Max(),
			},
		},
	}

	m.pageDocs = 0

	buf, err := m.ts.Write(context.TODO(), ph)
	if err != nil {
		return err
	}
	if err := m.updateRowGroup(pth, dataLen, compressedLen, len(buf), count, comp); err != nil {
		return err
	}

	_, err = w.Write(buf)
	return err
}

// Pages maps each column name to its Pages
func (m *Metadata) Pages() (map[string][]Page, error) {
	if len(m.metadata.RowGroups) == 0 {
		return nil, nil
	}
	out := map[string][]Page{}
	for _, rg := range m.metadata.RowGroups {
		for _, ch := range rg.Columns {
			pth := ch.MetaData.PathInSchema
			_, ok := m.schema.lookup[strings.Join(pth, ".")]
			if !ok {
				return nil, fmt.Errorf("could not find schema for %v", pth)
			}

			pg := Page{
				N:      int(ch.MetaData.NumValues),
				Offset: ch.FileOffset,
				Size:   int(ch.MetaData.TotalCompressedSize),
				Codec:  ch.MetaData.Codec,
			}
			k := strings.Join(pth, ".")
			out[k] = append(out[k], pg)
		}
	}
	return out, nil
}

func (m *Metadata) updateRowGroup(pth []string, dataLen, compressedLen, headerLen, count int, comp sch.CompressionCodec) error {
	i := len(m.rowGroups)
	if i == 0 {
		return fmt.Errorf("no row groups, you must call StartRowGroup at least once")
	}

	rg := m.rowGroups[i-1]

	rg.rowGroup.NumRows = m.rowGroupDocs
	err := rg.updateColumnChunk(pth, dataLen+headerLen, compressedLen+headerLen, count, m.schema, comp)
	m.rowGroups[i-1] = rg
	return err
}

func columnType(col string, fields schema) (sch.Type, error) {
	f, ok := fields.lookup[col]
	if !ok {
		return 0, fmt.Errorf("could not find type for column %s", col)
	}
	return *f.Type, nil
}

// Rows return the total number of rows that are being written
// in to a parquet file.
func (m *Metadata) Rows() int64 {
	return m.metadata.NumRows
}

// Footer writes the FileMetaData at the end of the file.
func (m *Metadata) Footer(w io.Writer) error {
	_, s := m.schema.schema()
	fmd := &sch.FileMetaData{
		Version:   1,
		Schema:    s,
		NumRows:   m.docs,
		RowGroups: make([]*sch.RowGroup, 0, len(m.rowGroups)),
	}

	pos := int64(4)
	for _, mrg := range m.rowGroups {
		rg := mrg.rowGroup
		if rg.NumRows == 0 {
			continue
		}

		for _, col := range mrg.fields.fields {
			ch, ok := mrg.columns[strings.Join(col.Path, ".")]
			if !ok {
				continue
			}

			ch.FileOffset = pos
			ch.MetaData.DataPageOffset = pos
			rg.TotalByteSize += ch.MetaData.TotalCompressedSize
			rg.Columns = append(rg.Columns, &ch)
			pos += ch.MetaData.TotalCompressedSize
		}

		fmd.RowGroups = append(fmd.RowGroups, &rg)
	}
	buf, err := m.ts.Write(context.TODO(), fmd)
	if err != nil {
		return err
	}

	n, err := w.Write(buf)
	if err != nil {
		return err
	}

	return binary.Write(w, binary.LittleEndian, uint32(n))
}


// ReadFooter reads the parquet metadata
func (m *Metadata) ReadFooter(r io.ReadSeeker) error {
	meta, err := ReadMetaData(r)
	m.metadata = meta
	return err
}


// New returns a Metadata struct and reads the first row group
// into memory.
func New(fields ...Field) *Metadata {
	ts := thrift.NewTSerializer()
	ts.Protocol = thrift.NewTCompactProtocolFactory().GetProtocol(ts.Transport)
	m := &Metadata{
		ts:     ts,
		schema: schemaElements(fields),
	}

	m.StartRowGroup(fields...)
	return m
}



// ReadMetaData reads the FileMetaData from the end of a parquet file
func ReadMetaData(r io.ReadSeeker) (*sch.FileMetaData, error) {
	p := thrift.NewTCompactProtocol(&thrift.StreamTransport{Reader: r})
	size, err := getMetaDataSize(r)
	if err != nil {
		return nil, err
	}

	_, err = r.Seek(-int64(size+8), io.SeekEnd)
	if err != nil {
		return nil, err
	}

	m := sch.NewFileMetaData()
	return m, m.Read(p)
}


func getMetaDataSize(r io.ReadSeeker) (int, error) {
	_, err := r.Seek(-8, io.SeekEnd)
	if err != nil {
		return 0, err
	}

	var size uint32
	return int(size), binary.Read(r, binary.LittleEndian, &size)
}
