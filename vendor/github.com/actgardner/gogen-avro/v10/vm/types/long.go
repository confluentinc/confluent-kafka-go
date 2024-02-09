package types

type Long struct {
	Target *int64
}

func (b Long) SetBoolean(v bool) {
	panic("Unable to assign boolean to long field")
}

func (b Long) SetInt(v int32) {
	*(b.Target) = int64(v)
}

func (b Long) SetLong(v int64) {
	*(b.Target) = v
}

func (b Long) SetFloat(v float32) {
	panic("Unable to assign float to long field")
}

func (b Long) SetUnionElem(v int64) {
	panic("Unable to assign union elem to long field")
}

func (b Long) SetDouble(v float64) {
	panic("Unable to assign double to long field")
}

func (b Long) SetBytes(v []byte) {
	panic("Unable to assign bytes to long field")
}

func (b Long) SetString(v string) {
	panic("Unable to assign string to long field")
}

func (b Long) Get(i int) Field {
	panic("Unable to get field from long field")
}

func (b Long) SetDefault(i int) {
	panic("Unable to set default on long field")
}

func (b Long) AppendMap(key string) Field {
	panic("Unable to append map key to from long field")
}

func (b Long) AppendArray() Field {
	panic("Unable to append array element to from long field")
}

func (b Long) NullField(int) {
	panic("Unable to null field in long field")
}

func (b Long) HintSize(int) {
	panic("Unable to hint size in long field")
}

func (b Long) Finalize() {}
