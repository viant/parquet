package filter

type SampledSubject struct {
	CreativeID int    `parquet:"name=creativeID,type=INT64"`
	AudienceID int    `parquet:"name=audienceID,type=INT64"`
	AdOrderID  int    `parquet:"name=adOrderID,type=INT64"`
	Filter     string `parquet:"name=filter,type=BYTE_ARRAY"`
	Passed     bool   `parquet:"name=passed,type=BOOLEAN"`
	Sampled    bool   `parquet:"name=sampled,type=BOOLEAN"`
}
