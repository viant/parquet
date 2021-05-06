package codegen

import (
	"github.com/viant/toolbox"
	"os"
	"path"
	"path/filepath"
	"strings"
)

//session stores generated codes, imports, Pkg
type session struct {
	*Options
	*toolbox.FileSetInfo
	Pkg                  string
	generatedTypes       map[string]bool
	generatedParquetType map[string]bool
	imports              map[string]bool
	fieldStructCode      []string
	fieldInitCode        []string
	accessorMutatorCode  []string
}

//shallGenerateCode stores generated codes for all types
func (s *session) shallGenerateCode(typeName string) bool {
	if _, ok := s.generatedTypes[typeName]; ok {
		return false
	}
	s.generatedTypes[typeName] = true
	return true
}

//shallGenerateCode stores generated codes for all types
func (s *session) shallGenerateParquetFieldType(typeName string) bool {
	if _, ok := s.generatedParquetType[typeName]; ok {
		return false
	}
	s.generatedParquetType[typeName] = true
	return true
}

//readPackageCode creates Pkg code
func (s *session) readPackageCode() error {
	p, err := filepath.Abs(s.Source)
	if err != nil {
		return err
	}

	var f os.FileInfo
	if f, err = os.Stat(p); err != nil {
		// path/to/whatever does not exist
		return err
	}

	if !f.IsDir() {
		dir, _ := filepath.Split(p)
		_, pkg := path.Split(filepath.Base(dir))
		s.Pkg = pkg
		s.FileSetInfo, err = toolbox.NewFileSetInfo(dir)

	} else {
		_, pkg := path.Split(filepath.Base(p))
		s.Pkg = pkg
		s.FileSetInfo, err = toolbox.NewFileSetInfo(p)
	}

	// if Pkg flag is set use it
	if s.Pkg != "" {
		s.Pkg = s.Pkg
	}

	return err
}

func (s *session) addAccessorSnippet(snippet string) {
	s.accessorMutatorCode = append(s.accessorMutatorCode, snippet)
}

func (s *session) addMutatorSnippet(snippet string) {
	s.accessorMutatorCode = append(s.accessorMutatorCode, snippet)
}
func (s *session) addFieldStructSnippet(snippet string) {
	s.fieldStructCode = append(s.fieldStructCode, snippet)
}

func (s *session) addFieldInitSnippet(snippet string) {
	s.fieldInitCode = append(s.fieldInitCode, snippet)
}

//addImports adds imports
func (s *session) addImport(pkg string) {
	if !strings.Contains(pkg, " ") {
		s.imports[`"`+pkg+`"`] = true
		return
	}
	pair := strings.SplitN(pkg, " ", 2)
	s.imports[pair[0]+` "`+pair[1]+`"`] = true
}

//getImports returns imports
func (s *session) getImports() string {
	return "\t" + strings.Join(toolbox.MapKeysToStringSlice(s.imports), "\n\t")
}

//newSession creates a new session
func newSession(option *Options) *session {
	return &session{Options: option,
		fieldStructCode:      make([]string, 0),
		generatedTypes:       make(map[string]bool),
		imports:              make(map[string]bool),
		generatedParquetType: make(map[string]bool),
	}
}
