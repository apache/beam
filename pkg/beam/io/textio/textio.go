package textio

import (
	"bufio"
	"io/ioutil"
	"log"
	"os"
	"path/filepath"
	"reflect"

	"github.com/apache/beam/sdks/go/pkg/beam"
)

func init() {
	beam.RegisterType(reflect.TypeOf((*readFileFn)(nil)).Elem())
	beam.RegisterType(reflect.TypeOf((*writeFileFn)(nil)).Elem())
}

// Read reads a local file and returns the lines as a PCollection<string>. The
// newlines are not part of the lines.
func Read(p *beam.Pipeline, filename string) beam.PCollection {
	p = p.Composite("textio.Read")
	return beam.Source(p, &readFileFn{Filename: filename})
}

type readFileFn struct {
	Filename string `json:"filename"`
}

func (r *readFileFn) ProcessElement(emit func(string)) error {
	log.Printf("Reading from %v", r.Filename)

	file, err := os.Open(r.Filename)
	if err != nil {
		return err
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		emit(scanner.Text())
	}
	return scanner.Err()
}

// Write writes a PCollection<string> to a local file as separate lines. The
// writer add a newline after each element.
func Write(p *beam.Pipeline, filename string, col beam.PCollection) {
	p = p.Composite("textio.Write")
	beam.Sink(p, &writeFileFn{Filename: filename}, col)
}

type writeFileFn struct {
	Filename string `json:"filename"`

	writer *bufio.Writer
	fd     *os.File
}

func (w *writeFileFn) Setup() error {
	if err := os.MkdirAll(filepath.Dir(w.Filename), 0755); err != nil {
		return err
	}
	fd, err := ioutil.TempFile(filepath.Dir(w.Filename), filepath.Base(w.Filename))
	if err != nil {
		return err
	}
	log.Printf("Writing to %v", fd.Name())

	w.fd = fd
	w.writer = bufio.NewWriterSize(fd, 1<<20)
	return nil
}

func (w *writeFileFn) ProcessElement(line string) error {
	if _, err := w.writer.WriteString(line); err != nil {
		return err
	}
	_, err := w.writer.Write([]byte{'\n'})
	return err
}

func (w *writeFileFn) Teardown() error {
	if err := w.writer.Flush(); err != nil {
		return err
	}
	return w.fd.Close()
}

// Immediate reads a local file at pipeline construction-time and embeds the
// data into a I/O-free pipeline source. Should be used for small files only.
func Immediate(p *beam.Pipeline, filename string) (beam.PCollection, error) {
	p = p.Composite("textio.Immediate")

	var data []interface{}

	file, err := os.Open(filename)
	if err != nil {
		return beam.PCollection{}, err
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		data = append(data, scanner.Text())
	}
	if err := scanner.Err(); err != nil {
		return beam.PCollection{}, err
	}
	return beam.Create(p, data...), nil
}
