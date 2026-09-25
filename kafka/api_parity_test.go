/**
 * Copyright 2026 Confluent Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package kafka

import (
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"reflect"
	"sort"
	"strings"
	"testing"
)

// exportedMethods returns the exported methods declared on *receiver in the
// sources of this package, split into the ones that are deprecated and the
// ones that are not.
//
// The split is read from the doc comments rather than from a hand-kept list,
// so that deprecating a method is enough to release the wrappers from having
// to expose it.
func exportedMethods(t *testing.T, receiver string) (live, deprecated []string) {
	t.Helper()

	entries, err := os.ReadDir(".")
	if err != nil {
		t.Fatalf("Failed to read the package directory: %s", err)
	}

	fset := token.NewFileSet()
	for _, entry := range entries {
		name := entry.Name()
		if entry.IsDir() || !strings.HasSuffix(name, ".go") || strings.HasSuffix(name, "_test.go") {
			continue
		}

		// Parsing never builds the file, so the sources of every platform are
		// read whatever the build tags say.
		file, err := parser.ParseFile(fset, name, nil, parser.ParseComments|parser.SkipObjectResolution)
		if err != nil {
			t.Fatalf("Failed to parse %s: %s", name, err)
		}

		for _, decl := range file.Decls {
			funcDecl, ok := decl.(*ast.FuncDecl)
			if !ok || funcDecl.Recv == nil || len(funcDecl.Recv.List) != 1 {
				continue
			}
			starExpr, ok := funcDecl.Recv.List[0].Type.(*ast.StarExpr)
			if !ok {
				continue
			}
			ident, ok := starExpr.X.(*ast.Ident)
			if !ok || ident.Name != receiver || !funcDecl.Name.IsExported() {
				continue
			}

			if isDeprecated(funcDecl.Doc) {
				deprecated = append(deprecated, funcDecl.Name.Name)
			} else {
				live = append(live, funcDecl.Name.Name)
			}
		}
	}

	if len(live) == 0 {
		t.Fatalf("Found no exported method of *%s, the parsing is broken", receiver)
	}
	sort.Strings(live)
	sort.Strings(deprecated)
	return live, deprecated
}

// isDeprecated reports whether doc carries the conventional deprecation
// marker, a paragraph starting with "Deprecated:".
func isDeprecated(doc *ast.CommentGroup) bool {
	if doc == nil {
		return false
	}
	for _, line := range strings.Split(doc.Text(), "\n") {
		if strings.HasPrefix(line, "Deprecated:") {
			return true
		}
	}
	return false
}

// methodSet returns the names of the exported methods of the given type.
func methodSet(typ reflect.Type) map[string]bool {
	names := make(map[string]bool, typ.NumMethod())
	for i := 0; i < typ.NumMethod(); i++ {
		names[typ.Method(i).Name] = true
	}
	return names
}

// assertWraps verifies that wrapper exposes every non-deprecated exported
// method of the wrapped type. Only the names are compared: the wrappers take
// and return the typed message of their key and value parameters, so the
// signatures deliberately differ.
func assertWraps(t *testing.T, wrapped string, wrapper reflect.Type) {
	t.Helper()

	live, deprecated := exportedMethods(t, wrapped)
	exposed := methodSet(wrapper)

	var missing []string
	for _, name := range live {
		if !exposed[name] {
			missing = append(missing, name)
		}
	}
	if len(missing) > 0 {
		t.Errorf("%s does not expose %d of the %d public methods of %s: %s",
			wrapper, len(missing), len(live), wrapped, strings.Join(missing, ", "))
	}

	// Deprecated methods are deliberately left out, so they are only reported.
	var wrappedDeprecated []string
	for _, name := range deprecated {
		if exposed[name] {
			wrappedDeprecated = append(wrappedDeprecated, name)
		}
	}
	t.Logf("%s: %d public methods checked, %d deprecated skipped (%s), of which wrapped anyway: %s",
		wrapped, len(live), len(deprecated), strings.Join(deprecated, ", "),
		strings.Join(wrappedDeprecated, ", "))
}

// TestSerializingProducerWrapsProducer verifies that a SerializingProducer can
// be used in place of a Producer: every public method of Producer that is not
// deprecated is exposed by the wrapper too.
func TestSerializingProducerWrapsProducer(t *testing.T) {
	assertWraps(t, "Producer", reflect.TypeOf((*SerializingProducer[string, string])(nil)))
}

// TestDeserializingConsumerWrapsConsumer verifies that a DeserializingConsumer
// can be used in place of a Consumer: every public method of Consumer that is
// not deprecated is exposed by the wrapper too.
func TestDeserializingConsumerWrapsConsumer(t *testing.T) {
	assertWraps(t, "Consumer", reflect.TypeOf((*DeserializingConsumer[string, string])(nil)))
}
