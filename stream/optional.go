package stream

type Optional struct {
	Values
	defs   []uint8
	maxDef uint8
}

func (i *Optional) grow(delta int) {
	capacity := defaultCapacity
	if delta > capacity {
		capacity = delta
	}
	tmp := i.defs
	i.defs = make([]uint8, len(tmp)+capacity)
	copy(i.defs, tmp)
}
