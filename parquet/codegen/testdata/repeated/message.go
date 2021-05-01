package repeated

type Message struct {
	Sub []SubMessage
	ID *int
	Name *string
	Ints []int
	Floats []float64
	Strings []string
}


type SubMessage struct {
	Node Node
}

type Leaf struct {
	Ints []int
	Floats []float64
	Strings []string
}

type Node struct {
	Uints []uint
	Leafs []Leaf
}
