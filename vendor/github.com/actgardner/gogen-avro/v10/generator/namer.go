package generator

import (
	"regexp"
	"strings"
)

const (
	invalidTokensExpr = `[._\s]+`
)

// Namer is the interface defining a function for converting
// a name to a go-idiomatic public name.
type Namer interface {
	// ToPublicName returns a go-idiomatic public name. The Avro spec
	// specifies names must start with [A-Za-z_] and contain [A-Za-z0-9_].
	// The golang spec says valid identifiers start with [A-Za-z_] and contain
	// [A-Za-z0-9], but the first character must be [A-Z] for the field to be
	// public.
	ToPublicName(name string) string
}

// DefaultNamer implements the Namer interface with the
// backwards-compatible public name generator function.
type DefaultNamer struct {
}

// NamespaceNamer is like DefaultNamer but taking into account
// special tokens so namespaced names can be generated safely.
type NamespaceNamer struct {
	shortNames bool
	re         *regexp.Regexp
}

var (
	namer Namer = &DefaultNamer{}
)

// NewNamespaceNamer returns a namespace-aware namer.
func NewNamespaceNamer(shortNames bool) *NamespaceNamer {
	return &NamespaceNamer{shortNames: shortNames, re: regexp.MustCompile(invalidTokensExpr)}
}

// SetNamer sets the generator's global namer
func SetNamer(n Namer) {
	namer = n
}

// ToPublicName implements the backwards-compatible name converter in
// DefaultNamer.
func (d *DefaultNamer) ToPublicName(name string) string {
	return ToPublicSimpleName(name)
}

// ToPublicName implements the go-idiomatic public name as in DefaultNamer's
// struct, but with additional treatment applied in order to remove possible
// invalid tokens from it. Final string is then converted to camel-case.
func (n *NamespaceNamer) ToPublicName(name string) string {
	if n.shortNames {
		if parts := strings.Split(name, "."); len(parts) > 2 {
			name = strings.Join(parts[len(parts)-2:], ".")
		}
	}
	name = n.re.ReplaceAllString(name, " ")
	return strings.Replace(strings.Title(name), " ", "", -1)
}
