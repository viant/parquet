package poc

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"github.com/viant/parquet"
	sch "github.com/viant/parquet/schema"
	"io"
	"math"
	"sort"
	"strings"
)

type compression int

const (
	compressionUncompressed compression = 0
	compressionSnappy       compression = 1
	compressionUnknown      compression = -1
	compressionGZip         compression = 2
)

// ParquetWriter reprents a row group
type ParquetWriter struct {
	fields []Field

	len int

	// child points to the next page
	child *ParquetWriter

	// max is the number of Record items that can get written before
	// a new set of column chunks is written
	max int

	meta        *parquet.Metadata
	w           io.Writer
	compression compression
}

func Fields(compression compression) []Field {
	return []Field{
		NewStringOptionalField(readTimestamp, writeTimestamp, []string{"timestamp"}, []int{1}, optionalFieldCompression(compression)),
		NewStringOptionalField(readSID, writeSID, []string{"sid"}, []int{1}, optionalFieldCompression(compression)),
		NewStringOptionalField(readAuctionID, writeAuctionID, []string{"auctionId"}, []int{1}, optionalFieldCompression(compression)),
		NewStringOptionalField(readHostNodeIP, writeHostNodeIP, []string{"hostNodeIp"}, []int{1}, optionalFieldCompression(compression)),
		NewStringOptionalField(readHostNodeVersion, writeHostNodeVersion, []string{"hostNodeVersion"}, []int{1}, optionalFieldCompression(compression)),
		NewStringOptionalField(readEndpoint, writeEndpoint, []string{"endpoint"}, []int{1}, optionalFieldCompression(compression)),
		NewInt64OptionalField(readTimeTakenMcs, writeTimeTakenMcs, []string{"timeTakenMcs"}, []int{1}, optionalFieldCompression(compression)),
		NewStringOptionalField(readIP, writeIP, []string{"ip"}, []int{1}, optionalFieldCompression(compression)),
		NewInt64OptionalField(readPublisherID, writePublisherID, []string{"publisherID"}, []int{1}, optionalFieldCompression(compression)),
		NewInt64OptionalField(readSiteID, writeSiteID, []string{"siteId"}, []int{1}, optionalFieldCompression(compression)),
		NewBoolOptionalField(readFiltered, writeFiltered, []string{"filtered"}, []int{1}, optionalFieldCompression(compression)),
		NewBoolOptionalField(readAborted, writeAborted, []string{"aborted"}, []int{1}, optionalFieldCompression(compression)),
		NewBoolOptionalField(readThrottled, writeThrottled, []string{"throttled"}, []int{1}, optionalFieldCompression(compression)),
		NewStringOptionalField(readError, writeError, []string{"error"}, []int{1}, optionalFieldCompression(compression)),
		NewStringOptionalField(readTimeouts, writeTimeouts, []string{"timeouts"}, []int{2}, optionalFieldCompression(compression)),
		NewInt64OptionalField(readPassed, writePassed, []string{"passed"}, []int{1}, optionalFieldCompression(compression)),
		NewInt64OptionalField(readSelectableAdOrders, writeSelectableAdOrders, []string{"selectableAdOrders"}, []int{2}, optionalFieldCompression(compression)),
		NewInt64OptionalField(readSelectableAudiences, writeSelectableAudiences, []string{"selectableAudiences"}, []int{2}, optionalFieldCompression(compression)),
		NewInt64OptionalField(readPhase, writePhase, []string{"phase"}, []int{1}, optionalFieldCompression(compression)),
		NewInt64OptionalField(readSampledSubjectsCreativeID, writeSampledSubjectsCreativeID, []string{"sampledSubjects", "creativeID"}, []int{2, 1}, optionalFieldCompression(compression)),
		NewInt64OptionalField(readSampledSubjectsAudienceID, writeSampledSubjectsAudienceID, []string{"sampledSubjects", "audienceID"}, []int{2, 1}, optionalFieldCompression(compression)),
		NewInt64OptionalField(readSampledSubjectsAdOrderID, writeSampledSubjectsAdOrderID, []string{"sampledSubjects", "adOrderID"}, []int{2, 1}, optionalFieldCompression(compression)),
		NewStringOptionalField(readSampledSubjectsFilter, writeSampledSubjectsFilter, []string{"sampledSubjects", "filter"}, []int{2, 1}, optionalFieldCompression(compression)),
		NewBoolOptionalField(readSampledSubjectsPassed, writeSampledSubjectsPassed, []string{"sampledSubjects", "passed"}, []int{2, 1}, optionalFieldCompression(compression)),
		NewBoolOptionalField(readSampledSubjectsSampled, writeSampledSubjectsSampled, []string{"sampledSubjects", "sampled"}, []int{2, 1}, optionalFieldCompression(compression)),
	}
}

func readTimestamp(x Selection) ([]string, []uint8, []uint8) {
	switch {
	case x.Timestamp == nil:
		return nil, []uint8{0}, nil
	default:
		return []string{*x.Timestamp}, []uint8{1}, nil
	}
}

func writeTimestamp(x *Selection, vals []string, defs, reps []uint8) (int, int) {
	def := defs[0]
	switch def {
	case 1:
		x.Timestamp = pstring(vals[0])
		return 1, 1
	}

	return 0, 1
}

func readSID(x Selection) ([]string, []uint8, []uint8) {
	switch {
	case x.SID == nil:
		return nil, []uint8{0}, nil
	default:
		return []string{*x.SID}, []uint8{1}, nil
	}
}

func writeSID(x *Selection, vals []string, defs, reps []uint8) (int, int) {
	def := defs[0]
	switch def {
	case 1:
		x.SID = pstring(vals[0])
		return 1, 1
	}

	return 0, 1
}

func readAuctionID(x Selection) ([]string, []uint8, []uint8) {
	switch {
	case x.AuctionID == nil:
		return nil, []uint8{0}, nil
	default:
		return []string{*x.AuctionID}, []uint8{1}, nil
	}
}

func writeAuctionID(x *Selection, vals []string, defs, reps []uint8) (int, int) {
	def := defs[0]
	switch def {
	case 1:
		x.AuctionID = pstring(vals[0])
		return 1, 1
	}

	return 0, 1
}

func readHostNodeIP(x Selection) ([]string, []uint8, []uint8) {
	switch {
	case x.HostNodeIP == nil:
		return nil, []uint8{0}, nil
	default:
		return []string{*x.HostNodeIP}, []uint8{1}, nil
	}
}

func writeHostNodeIP(x *Selection, vals []string, defs, reps []uint8) (int, int) {
	def := defs[0]
	switch def {
	case 1:
		x.HostNodeIP = pstring(vals[0])
		return 1, 1
	}

	return 0, 1
}

func readHostNodeVersion(x Selection) ([]string, []uint8, []uint8) {
	switch {
	case x.HostNodeVersion == nil:
		return nil, []uint8{0}, nil
	default:
		return []string{*x.HostNodeVersion}, []uint8{1}, nil
	}
}

func writeHostNodeVersion(x *Selection, vals []string, defs, reps []uint8) (int, int) {
	def := defs[0]
	switch def {
	case 1:
		x.HostNodeVersion = pstring(vals[0])
		return 1, 1
	}

	return 0, 1
}

func readEndpoint(x Selection) ([]string, []uint8, []uint8) {
	switch {
	case x.Endpoint == nil:
		return nil, []uint8{0}, nil
	default:
		return []string{*x.Endpoint}, []uint8{1}, nil
	}
}

func writeEndpoint(x *Selection, vals []string, defs, reps []uint8) (int, int) {
	def := defs[0]
	switch def {
	case 1:
		x.Endpoint = pstring(vals[0])
		return 1, 1
	}

	return 0, 1
}

func readTimeTakenMcs(x Selection) ([]int64, []uint8, []uint8) {
	switch {
	case x.TimeTakenMcs == nil:
		return nil, []uint8{0}, nil
	default:
		return []int64{*x.TimeTakenMcs}, []uint8{1}, nil
	}
}

func writeTimeTakenMcs(x *Selection, vals []int64, defs, reps []uint8) (int, int) {
	def := defs[0]
	switch def {
	case 1:
		x.TimeTakenMcs = pint64(vals[0])
		return 1, 1
	}

	return 0, 1
}

func readIP(x Selection) ([]string, []uint8, []uint8) {
	switch {
	case x.IP == nil:
		return nil, []uint8{0}, nil
	default:
		return []string{*x.IP}, []uint8{1}, nil
	}
}

func writeIP(x *Selection, vals []string, defs, reps []uint8) (int, int) {
	def := defs[0]
	switch def {
	case 1:
		x.IP = pstring(vals[0])
		return 1, 1
	}

	return 0, 1
}

func readPublisherID(x Selection) ([]int64, []uint8, []uint8) {
	switch {
	case x.PublisherID == nil:
		return nil, []uint8{0}, nil
	default:
		return []int64{*x.PublisherID}, []uint8{1}, nil
	}
}

func writePublisherID(x *Selection, vals []int64, defs, reps []uint8) (int, int) {
	def := defs[0]
	switch def {
	case 1:
		x.PublisherID = pint64(vals[0])
		return 1, 1
	}

	return 0, 1
}

func readSiteID(x Selection) ([]int64, []uint8, []uint8) {
	switch {
	case x.SiteID == nil:
		return nil, []uint8{0}, nil
	default:
		return []int64{*x.SiteID}, []uint8{1}, nil
	}
}

func writeSiteID(x *Selection, vals []int64, defs, reps []uint8) (int, int) {
	def := defs[0]
	switch def {
	case 1:
		x.SiteID = pint64(vals[0])
		return 1, 1
	}

	return 0, 1
}

func readFiltered(x Selection) ([]bool, []uint8, []uint8) {
	switch {
	case x.Filtered == nil:
		return nil, []uint8{0}, nil
	default:
		return []bool{*x.Filtered}, []uint8{1}, nil
	}
}

func writeFiltered(x *Selection, vals []bool, defs, reps []uint8) (int, int) {
	def := defs[0]
	switch def {
	case 1:
		x.Filtered = pbool(vals[0])
		return 1, 1
	}

	return 0, 1
}

func readAborted(x Selection) ([]bool, []uint8, []uint8) {
	switch {
	case x.Aborted == nil:
		return nil, []uint8{0}, nil
	default:
		return []bool{*x.Aborted}, []uint8{1}, nil
	}
}

func writeAborted(x *Selection, vals []bool, defs, reps []uint8) (int, int) {
	def := defs[0]
	switch def {
	case 1:
		x.Aborted = pbool(vals[0])
		return 1, 1
	}

	return 0, 1
}

func readThrottled(x Selection) ([]bool, []uint8, []uint8) {
	switch {
	case x.Throttled == nil:
		return nil, []uint8{0}, nil
	default:
		return []bool{*x.Throttled}, []uint8{1}, nil
	}
}

func writeThrottled(x *Selection, vals []bool, defs, reps []uint8) (int, int) {
	def := defs[0]
	switch def {
	case 1:
		x.Throttled = pbool(vals[0])
		return 1, 1
	}

	return 0, 1
}

func readError(x Selection) ([]string, []uint8, []uint8) {
	switch {
	case x.Error == nil:
		return nil, []uint8{0}, nil
	default:
		return []string{*x.Error}, []uint8{1}, nil
	}
}

func writeError(x *Selection, vals []string, defs, reps []uint8) (int, int) {
	def := defs[0]
	switch def {
	case 1:
		x.Error = pstring(vals[0])
		return 1, 1
	}

	return 0, 1
}

func readTimeouts(x Selection) ([]string, []uint8, []uint8) {
	var vals []string
	var defs, reps []uint8
	var lastRep uint8

	if len(x.Timeouts) == 0 {
		defs = append(defs, 0)
		reps = append(reps, lastRep)
	} else {
		for i0, x0 := range x.Timeouts {
			if i0 == 1 {
				lastRep = 1
			}
			defs = append(defs, 1)
			reps = append(reps, lastRep)
			vals = append(vals, x0)
		}
	}

	return vals, defs, reps
}

func writeTimeouts(x *Selection, vals []string, defs, reps []uint8) (int, int) {
	var nVals, nLevels int
	ind := make(indices, 1)

	for i := range defs {
		def := defs[i]
		rep := reps[i]
		if i > 0 && rep == 0 {
			break
		}

		nLevels++
		ind.rep(rep)

		switch def {
		case 1:
			switch rep {
			case 0:
				x.Timeouts = []string{vals[nVals]}
			case 1:
				x.Timeouts = append(x.Timeouts, vals[nVals])
			}
			nVals++
		}
	}

	return nVals, nLevels
}

func readPassed(x Selection) ([]int64, []uint8, []uint8) {
	switch {
	case x.Passed == nil:
		return nil, []uint8{0}, nil
	default:
		return []int64{*x.Passed}, []uint8{1}, nil
	}
}

func writePassed(x *Selection, vals []int64, defs, reps []uint8) (int, int) {
	def := defs[0]
	switch def {
	case 1:
		x.Passed = pint64(vals[0])
		return 1, 1
	}

	return 0, 1
}

func readSelectableAdOrders(x Selection) ([]int64, []uint8, []uint8) {
	var vals []int64
	var defs, reps []uint8
	var lastRep uint8

	if len(x.SelectableAdOrders) == 0 {
		defs = append(defs, 0)
		reps = append(reps, lastRep)
	} else {
		for i0, x0 := range x.SelectableAdOrders {
			if i0 == 1 {
				lastRep = 1
			}
			defs = append(defs, 1)
			reps = append(reps, lastRep)
			vals = append(vals, x0)
		}
	}

	return vals, defs, reps
}

func writeSelectableAdOrders(x *Selection, vals []int64, defs, reps []uint8) (int, int) {
	var nVals, nLevels int
	ind := make(indices, 1)

	for i := range defs {
		def := defs[i]
		rep := reps[i]
		if i > 0 && rep == 0 {
			break
		}

		nLevels++
		ind.rep(rep)

		switch def {
		case 1:
			switch rep {
			case 0:
				x.SelectableAdOrders = []int64{vals[nVals]}
			case 1:
				x.SelectableAdOrders = append(x.SelectableAdOrders, vals[nVals])
			}
			nVals++
		}
	}

	return nVals, nLevels
}

func readSelectableAudiences(x Selection) ([]int64, []uint8, []uint8) {
	var vals []int64
	var defs, reps []uint8
	var lastRep uint8

	if len(x.SelectableAudiences) == 0 {
		defs = append(defs, 0)
		reps = append(reps, lastRep)
	} else {
		for i0, x0 := range x.SelectableAudiences {
			if i0 == 1 {
				lastRep = 1
			}
			defs = append(defs, 1)
			reps = append(reps, lastRep)
			vals = append(vals, x0)
		}
	}

	return vals, defs, reps
}

func writeSelectableAudiences(x *Selection, vals []int64, defs, reps []uint8) (int, int) {
	var nVals, nLevels int
	ind := make(indices, 1)

	for i := range defs {
		def := defs[i]
		rep := reps[i]
		if i > 0 && rep == 0 {
			break
		}

		nLevels++
		ind.rep(rep)

		switch def {
		case 1:
			switch rep {
			case 0:
				x.SelectableAudiences = []int64{vals[nVals]}
			case 1:
				x.SelectableAudiences = append(x.SelectableAudiences, vals[nVals])
			}
			nVals++
		}
	}

	return nVals, nLevels
}

func readPhase(x Selection) ([]int64, []uint8, []uint8) {
	switch {
	case x.Phase == nil:
		return nil, []uint8{0}, nil
	default:
		return []int64{*x.Phase}, []uint8{1}, nil
	}
}

func writePhase(x *Selection, vals []int64, defs, reps []uint8) (int, int) {
	def := defs[0]
	switch def {
	case 1:
		x.Phase = pint64(vals[0])
		return 1, 1
	}

	return 0, 1
}

func readSampledSubjectsCreativeID(x Selection) ([]int64, []uint8, []uint8) {
	var vals []int64
	var defs, reps []uint8
	var lastRep uint8

	if len(x.SampledSubjects) == 0 {
		defs = append(defs, 0)
		reps = append(reps, lastRep)
	} else {
		for i0, x0 := range x.SampledSubjects {
			if i0 == 1 {
				lastRep = 1
			}
			if x0.CreativeID == nil {
				defs = append(defs, 1)
				reps = append(reps, lastRep)
			} else {
				defs = append(defs, 2)
				reps = append(reps, lastRep)
				vals = append(vals, *x0.CreativeID)
			}
		}
	}

	return vals, defs, reps
}

func writeSampledSubjectsCreativeID(x *Selection, vals []int64, defs, reps []uint8) (int, int) {
	var nVals, nLevels int
	ind := make(indices, 1)

	for i := range defs {
		def := defs[i]
		rep := reps[i]
		if i > 0 && rep == 0 {
			break
		}

		nLevels++
		ind.rep(rep)

		switch def {
		case 1:
			x.SampledSubjects = append(x.SampledSubjects, SampledSubject{})
		case 2:
			switch rep {
			case 0:
				x.SampledSubjects = []SampledSubject{{CreativeID: pint64(vals[nVals])}}
			case 1:
				x.SampledSubjects = append(x.SampledSubjects, SampledSubject{CreativeID: pint64(vals[nVals])})
			}
			nVals++
		}
	}

	return nVals, nLevels
}

func readSampledSubjectsAudienceID(x Selection) ([]int64, []uint8, []uint8) {
	var vals []int64
	var defs, reps []uint8
	var lastRep uint8

	if len(x.SampledSubjects) == 0 {
		defs = append(defs, 0)
		reps = append(reps, lastRep)
	} else {
		for i0, x0 := range x.SampledSubjects {
			if i0 == 1 {
				lastRep = 1
			}
			if x0.AudienceID == nil {
				defs = append(defs, 1)
				reps = append(reps, lastRep)
			} else {
				defs = append(defs, 2)
				reps = append(reps, lastRep)
				vals = append(vals, *x0.AudienceID)
			}
		}
	}

	return vals, defs, reps
}

func writeSampledSubjectsAudienceID(x *Selection, vals []int64, defs, reps []uint8) (int, int) {
	var nVals, nLevels int
	ind := make(indices, 1)

	for i := range defs {
		def := defs[i]
		rep := reps[i]
		if i > 0 && rep == 0 {
			break
		}

		nLevels++
		ind.rep(rep)

		switch def {
		case 2:
			switch rep {
			default:
				x.SampledSubjects[ind[0]].AudienceID = pint64(vals[nVals])
			}
			nVals++
		}
	}

	return nVals, nLevels
}

func readSampledSubjectsAdOrderID(x Selection) ([]int64, []uint8, []uint8) {
	var vals []int64
	var defs, reps []uint8
	var lastRep uint8

	if len(x.SampledSubjects) == 0 {
		defs = append(defs, 0)
		reps = append(reps, lastRep)
	} else {
		for i0, x0 := range x.SampledSubjects {
			if i0 == 1 {
				lastRep = 1
			}
			if x0.AdOrderID == nil {
				defs = append(defs, 1)
				reps = append(reps, lastRep)
			} else {
				defs = append(defs, 2)
				reps = append(reps, lastRep)
				vals = append(vals, *x0.AdOrderID)
			}
		}
	}

	return vals, defs, reps
}

func writeSampledSubjectsAdOrderID(x *Selection, vals []int64, defs, reps []uint8) (int, int) {
	var nVals, nLevels int
	ind := make(indices, 1)

	for i := range defs {
		def := defs[i]
		rep := reps[i]
		if i > 0 && rep == 0 {
			break
		}

		nLevels++
		ind.rep(rep)

		switch def {
		case 2:
			switch rep {
			default:
				x.SampledSubjects[ind[0]].AdOrderID = pint64(vals[nVals])
			}
			nVals++
		}
	}

	return nVals, nLevels
}

func readSampledSubjectsFilter(x Selection) ([]string, []uint8, []uint8) {
	var vals []string
	var defs, reps []uint8
	var lastRep uint8

	if len(x.SampledSubjects) == 0 {
		defs = append(defs, 0)
		reps = append(reps, lastRep)
	} else {
		for i0, x0 := range x.SampledSubjects {
			if i0 == 1 {
				lastRep = 1
			}
			if x0.Filter == nil {
				defs = append(defs, 1)
				reps = append(reps, lastRep)
			} else {
				defs = append(defs, 2)
				reps = append(reps, lastRep)
				vals = append(vals, *x0.Filter)
			}
		}
	}

	return vals, defs, reps
}

func writeSampledSubjectsFilter(x *Selection, vals []string, defs, reps []uint8) (int, int) {
	var nVals, nLevels int
	ind := make(indices, 1)

	for i := range defs {
		def := defs[i]
		rep := reps[i]
		if i > 0 && rep == 0 {
			break
		}

		nLevels++
		ind.rep(rep)

		switch def {
		case 2:
			switch rep {
			default:
				x.SampledSubjects[ind[0]].Filter = pstring(vals[nVals])
			}
			nVals++
		}
	}

	return nVals, nLevels
}

func readSampledSubjectsPassed(x Selection) ([]bool, []uint8, []uint8) {
	var vals []bool
	var defs, reps []uint8
	var lastRep uint8

	if len(x.SampledSubjects) == 0 {
		defs = append(defs, 0)
		reps = append(reps, lastRep)
	} else {
		for i0, x0 := range x.SampledSubjects {
			if i0 == 1 {
				lastRep = 1
			}
			if x0.Passed == nil {
				defs = append(defs, 1)
				reps = append(reps, lastRep)
			} else {
				defs = append(defs, 2)
				reps = append(reps, lastRep)
				vals = append(vals, *x0.Passed)
			}
		}
	}

	return vals, defs, reps
}

func writeSampledSubjectsPassed(x *Selection, vals []bool, defs, reps []uint8) (int, int) {
	var nVals, nLevels int
	ind := make(indices, 1)

	for i := range defs {
		def := defs[i]
		rep := reps[i]
		if i > 0 && rep == 0 {
			break
		}

		nLevels++
		ind.rep(rep)

		switch def {
		case 2:
			switch rep {
			default:
				x.SampledSubjects[ind[0]].Passed = pbool(vals[nVals])
			}
			nVals++
		}
	}

	return nVals, nLevels
}

func readSampledSubjectsSampled(x Selection) ([]bool, []uint8, []uint8) {
	var vals []bool
	var defs, reps []uint8
	var lastRep uint8

	if len(x.SampledSubjects) == 0 {
		defs = append(defs, 0)
		reps = append(reps, lastRep)
	} else {
		for i0, x0 := range x.SampledSubjects {
			if i0 == 1 {
				lastRep = 1
			}
			if x0.Sampled == nil {
				defs = append(defs, 1)
				reps = append(reps, lastRep)
			} else {
				defs = append(defs, 2)
				reps = append(reps, lastRep)
				vals = append(vals, *x0.Sampled)
			}
		}
	}

	return vals, defs, reps
}

func writeSampledSubjectsSampled(x *Selection, vals []bool, defs, reps []uint8) (int, int) {
	var nVals, nLevels int
	ind := make(indices, 1)

	for i := range defs {
		def := defs[i]
		rep := reps[i]
		if i > 0 && rep == 0 {
			break
		}

		nLevels++
		ind.rep(rep)

		switch def {
		case 2:
			switch rep {
			default:
				x.SampledSubjects[ind[0]].Sampled = pbool(vals[nVals])
			}
			nVals++
		}
	}

	return nVals, nLevels
}

func fieldCompression(c compression) func(*parquet.RequiredField) {
	switch c {
	case compressionUncompressed:
		return parquet.RequiredFieldUncompressed
	case compressionSnappy:
		return parquet.RequiredFieldSnappy
	case compressionGZip:
		return parquet.RequiredFieldGZIP
	default:
		return parquet.RequiredFieldUncompressed
	}
}

func optionalFieldCompression(c compression) func(*parquet.OptionalField) {
	switch c {
	case compressionUncompressed:
		return parquet.OptionalFieldUncompressed
	case compressionSnappy:
		return parquet.OptionalFieldSnappy
	case compressionGZip:
		return parquet.OptionalFieldGZIP
	default:
		return parquet.OptionalFieldUncompressed
	}
}

func NewParquetWriter(w io.Writer, opts ...func(*ParquetWriter) error) (*ParquetWriter, error) {
	return newParquetWriter(w, append(opts, begin)...)
}

func newParquetWriter(w io.Writer, opts ...func(*ParquetWriter) error) (*ParquetWriter, error) {
	p := &ParquetWriter{
		max:         1000,
		w:           w,
		compression: compressionSnappy,
	}

	for _, opt := range opts {
		if err := opt(p); err != nil {
			return nil, err
		}
	}

	p.fields = Fields(p.compression)
	if p.meta == nil {
		ff := Fields(p.compression)
		schema := make([]parquet.Field, len(ff))
		for i, f := range ff {
			schema[i] = f.Schema()
		}
		p.meta = parquet.New(schema...)
	}

	return p, nil
}

// MaxPageSize is the maximum number of rows in each row groups' page.
func MaxPageSize(m int) func(*ParquetWriter) error {
	return func(p *ParquetWriter) error {
		p.max = m
		return nil
	}
}

func begin(p *ParquetWriter) error {
	_, err := p.w.Write([]byte("PAR1"))
	return err
}

func withMeta(m *parquet.Metadata) func(*ParquetWriter) error {
	return func(p *ParquetWriter) error {
		p.meta = m
		return nil
	}
}

func Uncompressed(p *ParquetWriter) error {
	p.compression = compressionUncompressed
	return nil
}

func Snappy(p *ParquetWriter) error {
	p.compression = compressionSnappy
	return nil
}

func Gzip(p *ParquetWriter) error {
	p.compression = compressionGZip
	return nil
}

func withCompression(c compression) func(*ParquetWriter) error {
	return func(p *ParquetWriter) error {
		p.compression = c
		return nil
	}
}

func (p *ParquetWriter) Write() error {
	for i, f := range p.fields {
		if err := f.Write(p.w, p.meta); err != nil {
			return err
		}

		for child := p.child; child != nil; child = child.child {
			if err := child.fields[i].Write(p.w, p.meta); err != nil {
				return err
			}
		}
	}

	p.fields = Fields(p.compression)
	p.child = nil
	p.len = 0

	schema := make([]parquet.Field, len(p.fields))
	for i, f := range p.fields {
		schema[i] = f.Schema()
	}
	p.meta.StartRowGroup(schema...)
	return nil
}

func (p *ParquetWriter) Close() error {
	if err := p.meta.Footer(p.w); err != nil {
		return err
	}

	_, err := p.w.Write([]byte("PAR1"))
	return err
}

func (p *ParquetWriter) Add(rec Selection) {
	if p.len == p.max {
		if p.child == nil {
			// an error can't happen here
			p.child, _ = newParquetWriter(p.w, MaxPageSize(p.max), withMeta(p.meta), withCompression(p.compression))
		}

		p.child.Add(rec)
		return
	}

	p.meta.NextDoc()
	for _, f := range p.fields {
		f.Add(rec)
	}

	p.len++
}

type Field interface {
	Add(r Selection)
	Write(w io.Writer, meta *parquet.Metadata) error
	Schema() parquet.Field
	Scan(r *Selection)
	Read(r io.ReadSeeker, pg parquet.Page) error
	Name() string
	Levels() ([]uint8, []uint8)
}

func getFields(ff []Field) map[string]Field {
	m := make(map[string]Field, len(ff))
	for _, f := range ff {
		m[f.Name()] = f
	}
	return m
}

func NewParquetReader(r io.ReadSeeker, opts ...func(*ParquetReader)) (*ParquetReader, error) {
	ff := Fields(compressionUnknown)
	pr := &ParquetReader{
		r: r,
	}

	for _, opt := range opts {
		opt(pr)
	}

	schema := make([]parquet.Field, len(ff))
	for i, f := range ff {
		pr.fieldNames = append(pr.fieldNames, f.Name())
		schema[i] = f.Schema()
	}

	meta := parquet.New(schema...)
	if err := meta.ReadFooter(r); err != nil {
		return nil, err
	}
	pr.rows = meta.Rows()
	var err error
	pr.pages, err = meta.Pages()
	if err != nil {
		return nil, err
	}

	pr.rowGroups = meta.RowGroups()
	_, err = r.Seek(4, io.SeekStart)
	if err != nil {
		return nil, err
	}
	pr.meta = meta

	return pr, pr.readRowGroup()
}

func readerIndex(i int) func(*ParquetReader) {
	return func(p *ParquetReader) {
		p.index = i
	}
}

// ParquetReader reads one page from a row group.
type ParquetReader struct {
	fields         map[string]Field
	fieldNames     []string
	index          int
	cursor         int64
	rows           int64
	rowGroupCursor int64
	rowGroupCount  int64
	pages          map[string][]parquet.Page
	meta           *parquet.Metadata
	err            error

	r         io.ReadSeeker
	rowGroups []parquet.RowGroup
}

type Levels struct {
	Name string
	Defs []uint8
	Reps []uint8
}

func (p *ParquetReader) Levels() []Levels {
	var out []Levels
	//for {
	for _, name := range p.fieldNames {
		f := p.fields[name]
		d, r := f.Levels()
		out = append(out, Levels{Name: f.Name(), Defs: d, Reps: r})
	}
	//	if err := p.readRowGroup(); err != nil {
	//		break
	//	}
	//}
	return out
}

func (p *ParquetReader) Error() error {
	return p.err
}

func (p *ParquetReader) readRowGroup() error {
	p.rowGroupCursor = 0

	if len(p.rowGroups) == 0 {
		p.rowGroupCount = 0
		return nil
	}

	rg := p.rowGroups[0]
	p.fields = getFields(Fields(compressionUnknown))
	p.rowGroupCount = rg.Rows
	p.rowGroupCursor = 0
	for _, col := range rg.Columns() {
		name := strings.Join(col.MetaData.PathInSchema, ".")
		f, ok := p.fields[name]
		if !ok {
			return fmt.Errorf("unknown field: %s", name)
		}
		pages := p.pages[name]
		if len(pages) <= p.index {
			break
		}

		pg := pages[0]
		if err := f.Read(p.r, pg); err != nil {
			return fmt.Errorf("unable to read field %s, err: %s", f.Name(), err)
		}
		p.pages[name] = p.pages[name][1:]
	}
	p.rowGroups = p.rowGroups[1:]
	return nil
}

func (p *ParquetReader) Rows() int64 {
	return p.rows
}

func (p *ParquetReader) Next() bool {
	if p.err == nil && p.cursor >= p.rows {
		return false
	}
	if p.rowGroupCursor >= p.rowGroupCount {
		p.err = p.readRowGroup()
		if p.err != nil {
			return false
		}
	}

	p.cursor++
	p.rowGroupCursor++
	return true
}

func (p *ParquetReader) Scan(x *Selection) {
	if p.err != nil {
		return
	}

	for _, name := range p.fieldNames {
		f := p.fields[name]
		f.Scan(x)
	}
}

type StringOptionalField struct {
	parquet.OptionalField
	vals  []string
	read  func(r Selection) ([]string, []uint8, []uint8)
	write func(r *Selection, vals []string, def, rep []uint8) (int, int)
	stats *stringOptionalStats
}

func NewStringOptionalField(read func(r Selection) ([]string, []uint8, []uint8), write func(r *Selection, vals []string, defs, reps []uint8) (int, int), path []string, types []int, opts ...func(*parquet.OptionalField)) *StringOptionalField {
	return &StringOptionalField{
		read:          read,
		write:         write,
		OptionalField: parquet.NewOptionalField(path, types, opts...),
		stats:         newStringOptionalStats(maxDef(types)),
	}
}

func (f *StringOptionalField) Schema() parquet.Field {
	return parquet.Field{Name: f.Name(), Path: f.Path(), Type: StringType, RepetitionType: f.RepetitionType, Types: f.Types}
}

func (f *StringOptionalField) Add(r Selection) {
	vals, defs, reps := f.read(r)
	f.stats.add(vals, defs)
	f.vals = append(f.vals, vals...)
	f.Defs = append(f.Defs, defs...)
	f.Reps = append(f.Reps, reps...)
}

func (f *StringOptionalField) Scan(r *Selection) {
	if len(f.Defs) == 0 {
		return
	}

	v, l := f.write(r, f.vals, f.Defs, f.Reps)
	f.vals = f.vals[v:]
	f.Defs = f.Defs[l:]
	if len(f.Reps) > 0 {
		f.Reps = f.Reps[l:]
	}
}

func (f *StringOptionalField) Write(w io.Writer, meta *parquet.Metadata) error {
	buf := bytes.Buffer{}

	for _, s := range f.vals {
		if err := binary.Write(&buf, binary.LittleEndian, int32(len(s))); err != nil {
			return err
		}
		buf.Write([]byte(s))
	}

	return f.DoWrite(w, meta, buf.Bytes(), len(f.Defs), f.stats)
}

func (f *StringOptionalField) Read(r io.ReadSeeker, pg parquet.Page) error {
	rr, _, err := f.DoRead(r, pg)
	if err != nil {
		return err
	}

	for j := 0; j < f.Values(); j++ {
		var x int32
		if err := binary.Read(rr, binary.LittleEndian, &x); err != nil {
			return err
		}
		s := make([]byte, x)
		if _, err := rr.Read(s); err != nil {
			return err
		}

		f.vals = append(f.vals, string(s))
	}
	return nil
}

func (f *StringOptionalField) Levels() ([]uint8, []uint8) {
	return f.Defs, f.Reps
}

type Int64OptionalField struct {
	parquet.OptionalField
	vals  []int64
	read  func(r Selection) ([]int64, []uint8, []uint8)
	write func(r *Selection, vals []int64, def, rep []uint8) (int, int)
	stats *int64optionalStats
}

func NewInt64OptionalField(read func(r Selection) ([]int64, []uint8, []uint8), write func(r *Selection, vals []int64, defs, reps []uint8) (int, int), path []string, types []int, opts ...func(*parquet.OptionalField)) *Int64OptionalField {
	return &Int64OptionalField{
		read:          read,
		write:         write,
		OptionalField: parquet.NewOptionalField(path, types, opts...),
		stats:         newint64optionalStats(maxDef(types)),
	}
}

func (f *Int64OptionalField) Schema() parquet.Field {
	return parquet.Field{Name: f.Name(), Path: f.Path(), Type: Int64Type, RepetitionType: f.RepetitionType, Types: f.Types}
}

func (f *Int64OptionalField) Write(w io.Writer, meta *parquet.Metadata) error {
	var buf bytes.Buffer
	for _, v := range f.vals {
		if err := binary.Write(&buf, binary.LittleEndian, v); err != nil {
			return err
		}
	}
	return f.DoWrite(w, meta, buf.Bytes(), len(f.Defs), f.stats)
}

func (f *Int64OptionalField) Read(r io.ReadSeeker, pg parquet.Page) error {
	rr, _, err := f.DoRead(r, pg)
	if err != nil {
		return err
	}

	v := make([]int64, f.Values()-len(f.vals))
	err = binary.Read(rr, binary.LittleEndian, &v)
	f.vals = append(f.vals, v...)
	return err
}

func (f *Int64OptionalField) Add(r Selection) {
	vals, defs, reps := f.read(r)
	f.stats.add(vals, defs)
	f.vals = append(f.vals, vals...)
	f.Defs = append(f.Defs, defs...)
	f.Reps = append(f.Reps, reps...)
}

func (f *Int64OptionalField) Scan(r *Selection) {
	if len(f.Defs) == 0 {
		return
	}

	v, l := f.write(r, f.vals, f.Defs, f.Reps)
	f.vals = f.vals[v:]
	f.Defs = f.Defs[l:]
	if len(f.Reps) > 0 {
		f.Reps = f.Reps[l:]
	}
}

func (f *Int64OptionalField) Levels() ([]uint8, []uint8) {
	return f.Defs, f.Reps
}

type BoolOptionalField struct {
	parquet.OptionalField
	vals  []bool
	read  func(r Selection) ([]bool, []uint8, []uint8)
	write func(r *Selection, vals []bool, defs, reps []uint8) (int, int)
	stats *boolOptionalStats
}

func NewBoolOptionalField(read func(r Selection) ([]bool, []uint8, []uint8), write func(r *Selection, vals []bool, defs, reps []uint8) (int, int), path []string, types []int, opts ...func(*parquet.OptionalField)) *BoolOptionalField {
	return &BoolOptionalField{
		read:          read,
		write:         write,
		OptionalField: parquet.NewOptionalField(path, types, opts...),
		stats:         newBoolOptionalStats(maxDef(types)),
	}
}

func (f *BoolOptionalField) Schema() parquet.Field {
	return parquet.Field{Name: f.Name(), Path: f.Path(), Type: BoolType, RepetitionType: f.RepetitionType, Types: f.Types}
}

func (f *BoolOptionalField) Read(r io.ReadSeeker, pg parquet.Page) error {
	rr, sizes, err := f.DoRead(r, pg)
	if err != nil {
		return err
	}

	v, err := parquet.GetBools(rr, f.Values()-len(f.vals), sizes)
	f.vals = append(f.vals, v...)
	return err
}

func (f *BoolOptionalField) Scan(r *Selection) {
	if len(f.Defs) == 0 {
		return
	}

	v, l := f.write(r, f.vals, f.Defs, f.Reps)
	f.vals = f.vals[v:]
	f.Defs = f.Defs[l:]
	if len(f.Reps) > 0 {
		f.Reps = f.Reps[l:]
	}
}

func (f *BoolOptionalField) Add(r Selection) {
	vals, defs, reps := f.read(r)
	f.stats.add(vals, defs)
	f.vals = append(f.vals, vals...)
	f.Defs = append(f.Defs, defs...)
	f.Reps = append(f.Reps, reps...)
}

func (f *BoolOptionalField) Write(w io.Writer, meta *parquet.Metadata) error {
	ln := len(f.vals)
	byteNum := (ln + 7) / 8
	rawBuf := make([]byte, byteNum)

	for i := 0; i < ln; i++ {
		if f.vals[i] {
			rawBuf[i/8] = rawBuf[i/8] | (1 << uint32(i%8))
		}
	}

	return f.DoWrite(w, meta, rawBuf, len(f.Defs), f.stats)
}

func (f *BoolOptionalField) Levels() ([]uint8, []uint8) {
	return f.Defs, f.Reps
}

type stringOptionalStats struct {
	vals   []string
	min    []byte
	max    []byte
	nils   int64
	maxDef uint8
}

func newStringOptionalStats(d uint8) *stringOptionalStats {
	return &stringOptionalStats{maxDef: d}
}

func (s *stringOptionalStats) add(vals []string, defs []uint8) {
	var i int
	for _, def := range defs {
		if def < s.maxDef {
			s.nils++
		} else {
			s.vals = append(s.vals, vals[i])
			i++
		}
	}
}

func (s *stringOptionalStats) NullCount() *int64 {
	return &s.nils
}

func (s *stringOptionalStats) DistinctCount() *int64 {
	return nil
}

func (s *stringOptionalStats) Min() []byte {
	if s.min == nil {
		s.minMax()
	}
	return s.min
}

func (s *stringOptionalStats) Max() []byte {
	if s.max == nil {
		s.minMax()
	}
	return s.max
}

func (s *stringOptionalStats) minMax() {
	if len(s.vals) == 0 {
		return
	}

	tmp := make([]string, len(s.vals))
	copy(tmp, s.vals)
	sort.Strings(tmp)
	s.min = []byte(tmp[0])
	s.max = []byte(tmp[len(tmp)-1])
}

type int64optionalStats struct {
	min     int64
	max     int64
	nils    int64
	nonNils int64
	maxDef  uint8
}

func newint64optionalStats(d uint8) *int64optionalStats {
	return &int64optionalStats{
		min:    int64(math.MaxInt64),
		maxDef: d,
	}
}

func (f *int64optionalStats) add(vals []int64, defs []uint8) {
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

func (f *int64optionalStats) bytes(val int64) []byte {
	var buf bytes.Buffer
	binary.Write(&buf, binary.LittleEndian, val)
	return buf.Bytes()
}

func (f *int64optionalStats) NullCount() *int64 {
	return &f.nils
}

func (f *int64optionalStats) DistinctCount() *int64 {
	return nil
}

func (f *int64optionalStats) Min() []byte {
	if f.nonNils == 0 {
		return nil
	}
	return f.bytes(f.min)
}

func (f *int64optionalStats) Max() []byte {
	if f.nonNils == 0 {
		return nil
	}
	return f.bytes(f.max)
}

type boolOptionalStats struct {
	maxDef uint8
	nils   int64
}

func newBoolOptionalStats(d uint8) *boolOptionalStats {
	return &boolOptionalStats{maxDef: d}
}

func (b *boolOptionalStats) add(vals []bool, defs []uint8) {
	for _, def := range defs {
		if def < b.maxDef {
			b.nils++
		}
	}
}

func (b *boolOptionalStats) NullCount() *int64 {
	return &b.nils
}

func (b *boolOptionalStats) DistinctCount() *int64 {
	return nil
}

func (b *boolOptionalStats) Min() []byte {
	return nil
}

func (b *boolOptionalStats) Max() []byte {
	return nil
}

func pint32(i int32) *int32       { return &i }
func puint32(i uint32) *uint32    { return &i }
func pint64(i int64) *int64       { return &i }
func puint64(i uint64) *uint64    { return &i }
func pbool(b bool) *bool          { return &b }
func pstring(s string) *string    { return &s }
func pfloat32(f float32) *float32 { return &f }
func pfloat64(f float64) *float64 { return &f }

// keeps track of the indices of repeated fields
// that have already been handled by a previous field
type indices []int

func (i indices) rep(rep uint8) {
	if rep > 0 {
		r := int(rep) - 1
		i[r] = i[r] + 1
		for j := int(rep); j < len(i); j++ {
			i[j] = 0
		}
	}
}

func maxDef(types []int) uint8 {
	var out uint8
	for _, typ := range types {
		if typ > 0 {
			out++
		}
	}
	return out
}

func Int32Type(se *sch.SchemaElement) {
	t := sch.Type_INT32
	se.Type = &t
}

func Uint32Type(se *sch.SchemaElement) {
	t := sch.Type_INT32
	se.Type = &t
	ct := sch.ConvertedType_UINT_32
	se.ConvertedType = &ct
}

func Int64Type(se *sch.SchemaElement) {
	t := sch.Type_INT64
	se.Type = &t
}

func Uint64Type(se *sch.SchemaElement) {
	t := sch.Type_INT64
	se.Type = &t
	ct := sch.ConvertedType_UINT_64
	se.ConvertedType = &ct
}

func Float32Type(se *sch.SchemaElement) {
	t := sch.Type_FLOAT
	se.Type = &t
}

func Float64Type(se *sch.SchemaElement) {
	t := sch.Type_DOUBLE
	se.Type = &t
}

func BoolType(se *sch.SchemaElement) {
	t := sch.Type_BOOLEAN
	se.Type = &t
}

func StringType(se *sch.SchemaElement) {
	t := sch.Type_BYTE_ARRAY
	se.Type = &t
}

