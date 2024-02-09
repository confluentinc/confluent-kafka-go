package types

type Float struct {
	Target *float32
}

func (b Float) SetBoolean(v bool) {
	panic("Unable to assign boolean to float field")
}

func (b Float) SetInt(v int32) {
	*(b.Target) = float32(v)
}

func (b Float) SetLong(v int64) {
	*(b.Target) = float32(v)
}

func (b Float) SetFloat(v float32) {
	*(b.Target) = v
}

func (b Float) SetUnionElem(v int64) {
	panic("Unable to assign union elem to float field")
}

func (b Float) SetDouble(v float64) {
	panic("Unable to assign double to float field")
}

func (b Float) SetBytes(v []byte) {
	panic("Unable to assign double to float field")
}

func (b Float) SetString(v string) {
	panic("Unable to assign double to float field")
}

func (b Float) Get(i int) Field {
	panic("Unable to get field from float field")
}

func (b Float) SetDefault(i int) {
	panic("Unable to set default on float field")
}

func (b Float) AppendMap(key string) Field {
	panic("Unable to append map key to from float field")
}

func (b Float) AppendArray() Field {
	panic("Unable to append array element to from float field")
}

func (b Float) NullField(int) {
	panic("Unable to null field in float field")
}

func (b Float) HintSize(int) {
	panic("Unable to hint size in float field")
}

func (b Float) Finalize() {}
