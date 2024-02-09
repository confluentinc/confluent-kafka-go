package types

type Record struct {
	Target Field
}

func (b Record) SetBoolean(v bool) {
	b.Target.SetBoolean(v)
}

func (b Record) SetInt(v int32) {
	b.Target.SetInt(v)
}

func (b Record) SetLong(v int64) {
	b.Target.SetLong(v)
}

func (b Record) SetFloat(v float32) {
	b.Target.SetFloat(v)
}

func (b Record) SetDouble(v float64) {
	b.Target.SetDouble(v)
}

func (b Record) SetBytes(v []byte) {
	b.Target.SetBytes(v)
}

func (b Record) SetString(v string) {
	b.Target.SetString(v)
}

func (b Record) Get(i int) Field {
	return b.Target.Get(i)
}

func (b Record) SetDefault(i int) {
	b.Target.SetDefault(i)
}

func (b Record) AppendMap(key string) Field {
	return b.Target.AppendMap(key)
}

func (b Record) AppendArray() Field {
	return b.Target.AppendArray()
}

func (b Record) NullField(v int) {
	b.Target.NullField(v)
}

func (b Record) HintSize(v int) {
	b.Target.HintSize(v)
}

func (b Record) Finalize() {
	b.Target.Finalize()
}
