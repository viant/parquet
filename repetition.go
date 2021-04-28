package parquet

// RepetitionType is an enum of the possible
// parquet repetition types
type RepetitionType int

const (
	Required RepetitionType = 0
	Optional RepetitionType = 1
	Repeated RepetitionType = 2
)

// RepetitionTypes provides several functions used by parquetgen's
// go templates to generate code.
type RepetitionTypes []RepetitionType

// MaxDef returns the largest definition level
func (r RepetitionTypes) MaxDef() uint8 {
	var out uint8
	for _, rt := range r {
		if rt == Optional || rt == Repeated {
			out++
		}
	}
	return out
}

// MaxRep returns the largest repetition level
func (r RepetitionTypes) MaxRep() uint8 {
	var out uint8
	for _, rt := range r {
		if rt == Repeated {
			out++
		}
	}
	return out
}

// Repeated figures out if there is a repeated field
func (r RepetitionTypes) Repeated() bool {
	for _, rt := range r {
		if rt == Repeated {
			return true
		}
	}
	return false
}

// Optional figures out if there is an optional field
func (r RepetitionTypes) Optional() bool {
	for _, rt := range r {
		if rt == Optional {
			return true
		}
	}
	return false
}

// Required figures out if there are no optional or repeated fields
func (r RepetitionTypes) Required() bool {
	for _, rt := range r {
		if rt != Required {
			return false
		}
	}
	return true
}

