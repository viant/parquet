package parquet

import (
	"bytes"
	"compress/gzip"
	"fmt"
	"github.com/viant/toolbox"
	"io"
	"io/ioutil"
	"strings"
	"time"
)

// GetBools reads a byte array and turns each bit into a bool
func GetBools(r io.Reader, n int, pageSizes []int) ([]bool, error) {
	var vals [8]bool
	data, _ := ioutil.ReadAll(r)
	out := make([]bool, 0, n)
	for _, nVals := range pageSizes {

		if nVals == 0 {
			continue
		}

		l := (nVals / 8)
		if nVals%8 > 0 {
			l++
		}

		var i int
		chunk := data[:l]
		data = data[l:]
		for _, b := range chunk {
			vals = unpackBools(b)
			m := min(nVals, 8)
			for j := 0; j < m; j++ {
				out = append(out, vals[j])
			}
			i += m
			nVals -= m
		}
	}
	return out, nil
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

func unpackBools(data byte) [8]bool {
	x := uint8(data)
	return [8]bool{
		(x>>0)&1 == 1,
		(x>>1)&1 == 1,
		(x>>2)&1 == 1,
		(x>>3)&1 == 1,
		(x>>4)&1 == 1,
		(x>>5)&1 == 1,
		(x>>6)&1 == 1,
		(x>>7)&1 == 1,
	}
}

func decodeGzip(r io.Reader) ([]byte, error) {
	reader, err := gzip.NewReader(r)
	if err != nil {
		return nil, err
	}
	defer reader.Close()
	return ioutil.ReadAll(reader)
}

func encodeGzip(b []byte) []byte {
	out := new(bytes.Buffer)
	writer := gzip.NewWriter(out)
	_, err := writer.Write(b)
	if err == nil {
		if err = writer.Flush(); err == nil {
			err = writer.Close()
		}
	}
	return out.Bytes()
}

func TimeToString(time time.Time) string {
	return time.Format("2006-01-02 15:04:05.000-07")
}

func StringToTime(ts string) *time.Time {
	layout := "2006-01-02 15:04:05.000"
	if strings.Contains(ts, "T") {
		layout = time.RFC3339Nano
	} else {
		layout = "2006-01-02 15:04:05.000-07"
	}
	t, err := toolbox.ToTime(ts, layout)
	if err != nil {
		fmt.Println("failed to convert time: %s %v\n", ts, err)
		return nil
	}
	return t
}
