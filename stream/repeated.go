package stream

type Repeated struct {
	Optional
	reps   []uint8
	maxRep uint8
}

func (i *Repeated) grow(delta int) {
	capacity := defaultCapacity
	if delta > capacity {
		capacity = delta
	}
	tmp := i.reps
	i.reps = make([]uint8, len(tmp)+capacity)
	copy(i.reps, tmp)
}
