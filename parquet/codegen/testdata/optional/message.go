package optional

type P struct {
	Key string
	Value string
	Keys []string
}

type I struct {
	T string
}

type S struct {
	I *I
}

type B struct {
	S []*S
}

type R struct {
	Ps []*P

}

type Message struct {
	R *R
	B *B
	Ts *string `parquet:"type=INT64,convertedType=TIMESTAMP_MILLIS"`
}

