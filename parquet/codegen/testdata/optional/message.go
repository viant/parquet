package optional


type P struct {
	Key string
	Value string
}

type R struct {
	Ps []*P
}

type Message struct {
	R *R
}

