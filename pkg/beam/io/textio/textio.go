package textio

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"log"
	"os"
	"reflect"
	"strings"

	"github.com/apache/beam/sdks/go/pkg/beam"
)

func init() {
	beam.RegisterType(reflect.TypeOf((*writeFileFn)(nil)).Elem())
}

// Read reads a set of file and returns the lines as a PCollection<string>. The
// newlines are not part of the lines.
func Read(p *beam.Pipeline, glob string) beam.PCollection {
	p = p.Composite("textio.Read")

	validateScheme(glob)
	return read(p, beam.Create(p, glob))
}

func validateScheme(glob string) {
	if strings.TrimSpace(glob) == "" {
		panic("empty file glob provided")
	}
	scheme := getScheme(glob)
	if _, ok := registry[scheme]; !ok {
		panic(fmt.Sprintf("textio scheme %v not registered", scheme))
	}
}

func getScheme(glob string) string {
	if index := strings.Index(glob, "://"); index > 0 {
		return glob[:index]
	}
	return "default"
}

func newFileSystem(ctx context.Context, glob string) (FileSystem, error) {
	scheme := getScheme(glob)
	mkfs, ok := registry[scheme]
	if !ok {
		return nil, fmt.Errorf("textio scheme %v not registered for %v", scheme, glob)
	}
	return mkfs(ctx), nil
}

// ReadAll expands and reads the filename given as globs by the incoming
// PCollection<string>. It returns the lines of all files as a single
// PCollection<string>. The newlines are not part of the lines.
func ReadAll(p *beam.Pipeline, col beam.PCollection) beam.PCollection {
	p = p.Composite("textio.ReadAll")

	return read(p, col)
}

func read(p *beam.Pipeline, col beam.PCollection) beam.PCollection {
	files := beam.ParDo(p, expandFn, col)
	return beam.ParDo(p, readFn, files)
}

func expandFn(ctx context.Context, glob string, emit func(string)) error {
	if strings.TrimSpace(glob) == "" {
		return nil // ignore empty string elements here
	}

	fs, err := newFileSystem(ctx, glob)
	if err != nil {
		return err
	}
	defer fs.Close()

	files, err := fs.List(ctx, glob)
	if err != nil {
		return err
	}
	for _, filename := range files {
		emit(filename)
	}
	return nil
}

func readFn(ctx context.Context, filename string, emit func(string)) error {
	log.Printf("Reading from %v", filename)

	fs, err := newFileSystem(ctx, filename)
	if err != nil {
		return err
	}
	defer fs.Close()

	fd, err := fs.OpenRead(ctx, filename)
	if err != nil {
		return err
	}
	defer fd.Close()

	scanner := bufio.NewScanner(fd)
	for scanner.Scan() {
		emit(scanner.Text())
	}
	return scanner.Err()
}

// TODO(herohde) 7/12/2017: extend Write to write to a series of files
// as well as allow sharding.

// Write writes a PCollection<string> to a file as separate lines. The
// writer add a newline after each element.
func Write(p *beam.Pipeline, filename string, col beam.PCollection) {
	p = p.Composite("textio.Write")

	validateScheme(filename)
	beam.ParDo0(p, &writeFileFn{Filename: filename}, col)
}

type writeFileFn struct {
	Filename string `json:"filename"`

	fs     FileSystem
	fd     io.WriteCloser
	writer *bufio.Writer
}

func (w *writeFileFn) Setup(ctx context.Context) error {
	fs, err := newFileSystem(ctx, w.Filename)
	if err != nil {
		return err
	}
	fd, err := fs.OpenWrite(ctx, w.Filename)
	if err != nil {
		fs.Close()
		return err
	}

	log.Printf("Writing to %v", w.Filename)

	w.fs = fs
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
	defer w.fs.Close()

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
