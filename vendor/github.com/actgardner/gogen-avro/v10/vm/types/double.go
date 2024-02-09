package types

type Double struct {
	Target *float64
}

func (b Double) SetBoolean(v bool) {
	panic("Unable to assign boolean to double field")
}

func (b Double) SetInt(v int32) {
	*(b.Target) = float64(v)
}

func (b Double) SetLong(v int64) {
	*(b.Target) = float64(v)
}

func (b Double) SetFloat(v float32) {
	*(b.Target) = float64(v)
}

func (b Double) SetDouble(v float64) {
	*(b.Target) = v
}

func (b Double) SetUnionElem(v int64) {
	panic("Unable to assign union elem to double field")
}

func (b Double) SetBytes(v []byte) {
	panic("Unable to assign bytes to double field")
}

func (b Double) SetString(v string) {
	panic("Unable to assign string to double field")
}

func (b Double) Get(i int) Field {
	panic("Unable to get field from double field")
}

func (b Double) SetDefault(i int) {
	panic("Unable to set default on double field")
}

func (b Double) AppendMap(key string) Field {
	panic("Unable to append map key to from double field")
}

func (b Double) AppendArray() Field {
	panic("Unable to append array element to from double field")
}

func (b Double) NullField(int) {
	panic("Unable to null field in double field")
}

func (b Double) HintSize(int) {
	panic("Unable to hint size in double field")
}

func (b Double) Finalize() {}
