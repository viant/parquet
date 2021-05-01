package parquet

// Field holds the type information for a parquet column
type Field struct {
	Name           string
	Path           []string
	Types          []int
	Options        []SchemeOption
}
