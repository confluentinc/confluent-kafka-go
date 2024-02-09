package schema

type Constructable interface {
	ConstructorMethod() string
}

func getConstructableForType(t AvroType) (Constructable, bool) {
	if c, ok := t.(Constructable); ok {
		return c, true
	}
	if ref, ok := t.(*Reference); ok {
		if c, ok := ref.Def.(Constructable); ok {
			return c, true
		}
	}
	return nil, false
}
