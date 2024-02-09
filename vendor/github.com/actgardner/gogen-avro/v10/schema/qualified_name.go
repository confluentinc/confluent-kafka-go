package schema

// QualifiedName represents an Avro qualified name, which includes an optional namespace and the type name.
type QualifiedName struct {
	Namespace string
	Name      string
}

func (q QualifiedName) String() string {
	if q.Namespace == "" {
		return q.Name
	}
	return q.Namespace + "." + q.Name
}
