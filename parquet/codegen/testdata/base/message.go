package base

import "time"


type Message struct {
	A1 string `parquet:"name=AField,convertedType=UTF8,logicalType=String"`
	B1 int    `parquet:"name=BField,convertedType=Int64"`
	C1 bool
	D1 []byte
	F1 float64
	H1 float32
	I1 int32
	J1 int64
	K1 uint32
	L1 uint64
	T1 time.Time
	A2 *string `parquet:"name=AField2,convertedType=UTF8,logicalType=String"`
	B2 *int    `parquet:"name=BField2,convertedType=Int64"`
	C2 *bool
	F2 *float64
	H2 *float32
	I2 *int32
	J2 *int64
	K2 *uint32
	L2 *uint64
	T2 *time.Time
}
