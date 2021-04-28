package codegen


func generateOptionalFieldStatStruct(sess *session, nodes Nodes) error {
	params := nodes.NewParams("")
	code, err := optionalStatStruct.Expand("optionalFieldStatStruct", params)
	if err != nil {
		return err
	}
	sess.addFieldStructSnippet(code)
	return nil
}

var optionalStatStruct = Template(`
type {{.ParquetType}}optionalStats struct {
	min     {{.ParquetType}}
	max     {{.ParquetType}}
	nils    int64
	nonNils int64
	maxDef  uint8
}

func new{{.ParquetType}}optionalStats(d uint8) *{{.ParquetType}}optionalStats {
	return &{{.ParquetType}}optionalStats{
		min:    {{.ParquetType}}(math.Max{{.ParquetTypeTitle}}),
		maxDef: d,
	}
}

func (f *{{.ParquetType}}optionalStats) add(vals []{{.ParquetType}}, defs []uint8) {
	var i int
	for _, def := range defs {
		if def < f.maxDef {
			f.nils++
		} else {
			val := vals[i]
			i++

			f.nonNils++
			if val < f.min {
				f.min = val
			}
			if val > f.max {
				f.max = val
			}
		}
	}
}

func (f *{{.ParquetType}}optionalStats) bytes(val {{.ParquetType}}) []byte {
	var buf bytes.Buffer
	binary.Write(&buf, binary.LittleEndian, val)
	return buf.Bytes()
}

func (f *{{.ParquetType}}optionalStats) NullCount() *int64 {
	return &f.nils
}

func (f *{{.ParquetType}}optionalStats) DistinctCount() *int64 {
	return nil
}

func (f *{{.ParquetType}}optionalStats) Min() []byte {
	if f.nonNils == 0 {
		return nil
	}
	return f.bytes(f.min)
}

func (f *{{.ParquetType}}optionalStats) Max() []byte {
	if f.nonNils == 0 {
		return nil
	}
	return f.bytes(f.max)
}
`)


func generateRequiredFieldStatStruct(sess *session, nodes Nodes) error {
	params := nodes.NewParams("")
	code, err := requiredStatStruct.Expand("requiredStatStruct", params)
	if err != nil {
		return err
	}
	sess.addFieldStructSnippet(code)
	return nil
}


var requiredStatStruct = Template(`

type {{.ParquetType}}stats struct {
	min {{.ParquetType}}
	max {{.ParquetType}}
}

func new{{.StructType}}stats() *{{.ParquetType}}stats {
	return &{{.ParquetType}}stats{
		min: {{.ParquetType}}(math.Max{{.ParquetTypeTitle}}),
	}
}

func (i *{{.ParquetType}}stats) add(val {{.ParquetType}}) {
	if val < i.min {
		i.min = val
	}
	if val > i.max {
		i.max = val
	}
}

func (f *{{.ParquetType}}stats) bytes(val {{.ParquetType}}) []byte {
	var buf bytes.Buffer
	binary.Write(&buf, binary.LittleEndian, val)
	return buf.Bytes()
}

func (f *{{.ParquetType}}stats) NullCount() *int64 {
	return nil
}

func (f *{{.ParquetType}}stats) DistinctCount() *int64 {
	return nil
}

func (f *{{.ParquetType}}stats) Min() []byte {
	return f.bytes(f.min)
}

func (f *{{.ParquetType}}stats) Max() []byte {
	return f.bytes(f.max)
}
`)