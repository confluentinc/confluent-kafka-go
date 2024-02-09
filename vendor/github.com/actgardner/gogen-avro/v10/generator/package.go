// Utility methods for managing and writing generated code
package generator

import (
	"fmt"
	gofmt "go/format"
	"io/ioutil"
	"path/filepath"
	"sort"
)

// Package represents the output package
type Package struct {
	name   string
	header string
	files  map[string]string
}

func NewPackage(name, header string) *Package {
	return &Package{name: name, header: header, files: make(map[string]string)}
}

func (p *Package) WriteFiles(targetDir string) error {
	for name, body := range p.files {
		targetFile := filepath.Join(targetDir, name)
		fileContent, err := gofmt.Source([]byte(fmt.Sprintf("%v\npackage %v\n%v", p.header, p.name, body)))
		if err != nil {
			return fmt.Errorf("Error writing file %v - %v", targetFile, err)
		}

		err = ioutil.WriteFile(targetFile, []byte(fileContent), 0640)
		if err != nil {
			return fmt.Errorf("Error writing file %v - %v", targetFile, err)
		}
	}
	return nil
}

func (p *Package) Files() []string {
	files := make([]string, 0)
	for file, _ := range p.files {
		files = append(files, file)
	}
	sort.Strings(files)
	return files
}

func (p *Package) HasFile(name string) bool {
	_, ok := p.files[name]
	return ok
}

func (p *Package) AddFile(name string, body string) {
	p.files[name] = body
}
