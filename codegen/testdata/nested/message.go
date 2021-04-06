package nested

type Message struct {
	S []SubMessage
}


type SubMessage struct {
	Leaf []Leaf
}

type Leaf struct {
	M []string
}
