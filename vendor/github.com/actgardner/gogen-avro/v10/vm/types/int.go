package types

type Int struct {
	Target *int32
}

func (b Int) SetBoolean(v bool) {
	panic("Unable to assign boolean to int field")
}

func (b Int) SetInt(v int32) {
	*(b.Target) = v
}

func (b Int) SetLong(v int64) {
	panic("Unable to assign long to int field")
}

func (b Int) SetFloat(v float32) {
	panic("Unable to assign float to int field")
}

func (b Int) SetUnionElem(v int64) {
	panic("Unable to assign union elem to int field")
}

func (b Int) SetDouble(v float64) {
	panic("Unable to assign double to int field")
}

func (b Int) SetBytes(v []byte) {
	panic("Unable to assign bytes to int field")
}

func (b Int) SetString(v string) {
	panic("Unable to assign string to int field")
}

func (b Int) Get(i int) Field {
	panic("Unable to get field from int field")
}

func (b Int) SetDefault(i int) {
	panic("Unable to set default on int field")
}

func (b Int) AppendMap(key string) Field {
	panic("Unable to append map key to from int field")
}

func (b Int) AppendArray() Field {
	panic("Unable to append array element to from int field")
}

func (b Int) NullField(int) {
	panic("Unable to null field in int field")
}

func (b Int) HintSize(int) {
	panic("Unable to hint size in int field")
}

func (b Int) Finalize() {}
