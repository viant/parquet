package stream

type Encoder struct {
	int32 EncInt32s
}

func (s *Encoder) Int32() *EncInt32s {
	return &s.int32
}
