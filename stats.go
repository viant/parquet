package parquet

// Stats is passed in by each column's call to DoWrite
type Stats interface {
	NullCount() *int64
	DistinctCount() *int64
	Min() []byte
	Max() []byte
}
