package stream

const (
	defaultCapacity = 32 * 1024
)

type Values struct {
	index int
	cap   int
}



func fill(dest []uint8, value uint8, size int) {

}


//_values dictionary
var _values = make([][]uint8, 128)
func init() {
	for i, _ := range _values {
		_values[i] = make([]uint8, 128)
		for j := 0;j<128;j++ {
			_values[i][j] = uint8(i)
		}
	}
}


