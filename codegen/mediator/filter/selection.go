package filter

type Selection struct {
	Timestamp           *string          `parquet:"timestamp"`
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
