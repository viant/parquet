package poc

type Selection struct {
	Timestamp           *int64           `parquet:"timestamp", `
	SID                 *string          `parquet:"sid"`
	AuctionID           *string          `parquet:"auctionId"`
	HostNodeIP          *string          `parquet:"hostNodeIp"`
	HostNodeVersion     *string          `parquet:"hostNodeVersion"`
	Endpoint            *string          `parquet:"endpoint"`
	TimeTakenMcs        *int64           `parquet:"timeTakenMcs"`
	IP                  *string          `parquet:"ip"`
	PublisherID         *int64           `parquet:"publisherID"`
	SiteID              *int64           `parquet:"siteId"`
	Filtered            *bool            `parquet:"filtered"`
	Aborted             *bool            `parquet:"aborted"`
	Throttled           *bool            `parquet:"throttled"`
	Error               *string          `parquet:"error"`
	Timeouts            []string         `parquet:"timeouts"`
	Passed              *int64           `parquet:"passed"`
	SelectableAdOrders  []int64          `parquet:"selectableAdOrders"`
	SelectableAudiences []int64          `parquet:"selectableAudiences"`
	Phase               *int64           `parquet:"phase"`
	SampledSubjects     []SampledSubject `parquet:"sampledSubjects"`
}


type SampledSubject struct {
	CreativeID *int64    `parquet:"creativeID"`
	AudienceID *int64    `parquet:"audienceID"`
	AdOrderID  *int64    `parquet:"adOrderID"`
	Filter     *string   `parquet:"filter"`
	Passed     *bool     `parquet:"passed"`
	Sampled    *bool     `parquet:"sampled"`
}