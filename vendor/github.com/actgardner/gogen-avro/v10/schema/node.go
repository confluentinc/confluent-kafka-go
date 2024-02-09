package schema

type Node interface {
	Name() string
	Children() []AvroType
}
