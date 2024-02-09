package avro

// CompatMode defines a compatiblity mode used for checking Avro
// type compatibility.
type CompatMode int

const (
	Backward CompatMode = 1 << iota
	Forward
	Transitive

	BackwardTransitive = Backward | Transitive
	ForwardTransitive  = Forward | Transitive
	Full               = Backward | Forward
	FullTransitive     = Full | Transitive
)

// String returns a string representation of m, one of the values defined
// in https://docs.confluent.io/current/schema-registry/avro.html#schema-evolution-and-compatibility.
// For example FullTransitive.String() returns "FULL_TRANSITIVE".
func (m CompatMode) String() string {
	var s string
	switch m &^ Transitive {
	case 0:
		return "NONE"
	case Backward:
		s = "BACKWARD"
	case Forward:
		s = "FORWARD"
	case Backward | Forward:
		s = "FULL"
	default:
		return "UNKNOWN"
	}
	if m&Transitive != 0 {
		s += "_TRANSITIVE"
	}
	return s
}

// ParseCompatMode returns the CompatMode from a string.
// It returns -1 if no matches are found.
func ParseCompatMode(s string) CompatMode {
	switch s {
	case "BACKWARD":
		return Backward
	case "FORWARD":
		return Forward
	case "FULL":
		return Full
	case "BACKWARD_TRANSITIVE":
		return BackwardTransitive
	case "FORWARD_TRANSITIVE":
		return ForwardTransitive
	case "FULL_TRANSITIVE":
		return FullTransitive
	case "NONE":
		return 0
	}
	return -1
}
