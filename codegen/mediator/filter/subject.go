package filter

type SampledSubject struct {
	CreativeID *int64    `parquet:"creativeID"`
	AudienceID *int64    `parquet:"audienceID"`
	AdOrderID  *int64    `parquet:"adOrderID"`
	Filter     *string   `parquet:"filter"`
	Passed     *bool     `parquet:"passed"`
	Sampled    *bool     `parquet:"sampled"`
}
