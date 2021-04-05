package filter

type Selection struct {
	Timestamp           string           `parquet:"name=timestamp,type=BYTE_ARRAY,convertedtype=UTF8"`
	SID                 string           `parquet:"name=sid,type=BYTE_ARRAY,convertedtype=UTF8"`
	AuctionID           string           `parquet:"name=auctionId,type=BYTE_ARRAY,convertedtype=UTF8"`
	HostNodeIP          string           `parquet:"name=hostNodeIp,type=BYTE_ARRAY,convertedtype=UTF8"`
	HostNodeVersion     string           `parquet:"name=hostNodeVersion,type=BYTE_ARRAY,convertedtype=UTF8"`
	Endpoint            string           `parquet:"name=endpoint,type=BYTE_ARRAY,convertedtype=UTF8"`
	TimeTakenMcs        int              `parquet:"name=timeTakenMcs,type=INT64"`
	IP                  string           `parquet:"name=ip,type=BYTE_ARRAY,convertedtype=UTF8"`
	PublisherID         int              `parquet:"name=publisherID,type=INT64"`
	SiteID              int              `parquet:"name=siteId,type=INT64"`
	Filtered            bool             `parquet:"name=filtered,type=BOOLEAN"`
	Aborted             bool             `parquet:"name=aborted,type=BOOLEAN"`
	Throttled           bool             `parquet:"name=throttled,type=BOOLEAN"`
	Error               string           `parquet:"name=error,type=BYTE_ARRAY,convertedtype=UTF8"`
	Timeouts            []string         `parquet:"name=timeouts,type=BYTE_ARRAY,convertedtype=UTF8,repetitiontype=REPEATED"`
	Passed              int              `parquet:"name=passed,type=INT64"`
	SelectableAdOrders  []int            `parquet:"name=selectableAdOrders,type=INT64,repetitiontype=REPEATED"`
	SelectableAudiences []int            `parquet:"name=selectableAudiences,type=INT64,repetitiontype=REPEATED"`
	Phase               int              `parquet:"name=phase,type=INT64"`
	SampledSubjects     []SampledSubject `parquet:"name=sampledSubjects,repetitiontype=REPEATED"`
}
