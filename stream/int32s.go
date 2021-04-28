package stream

type EncInt32s struct {
	Repeated
	vals []int32
}

func (i *EncInt32s) Add(val int32) {
	i.growIfNeeded(1)
	i.vals[i.index] =val
	i.index++
}


func (i *EncInt32s) AddEmpty(def uint8) {

}

func (i *EncInt32s) Adds(val []int32, rep, def uint8) {
	size := len(val)
	i.growIfNeeded(size)
	i.reps[i.index] = rep
	i.defs[i.index] = def
	if size > 0 {
		copy(i.vals[i.index:], val)
		fill(i.reps[i.index+1:], i.maxRep, len(val)-1)
		fill(i.reps[i.index+1:], i.maxRep, len(val)-1)
	}
	i.index+=size
}






func (i *EncInt32s) growIfNeeded(delta int) {
	if i.cap >= i.index+delta {
		capacity := defaultCapacity
		if delta > capacity {
			capacity = delta
		}
		tmp := i.vals
		i.vals = make([]int32, len(tmp)+capacity)
		copy(i.vals, tmp)
		i.cap = len(i.vals)
		if i.maxRep > 0 {
			i.Repeated.grow(delta)
			i.Optional.grow(delta)
		} else if i.maxDef > 0 {
			i.Optional.grow(delta)
		}
	}
}
