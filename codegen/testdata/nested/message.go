package nested

type Message struct {
	Sub []SubMessage
}


type SubMessage struct {
	Leaf Leaf
}

type Leaf struct {
	Strings []string
}
