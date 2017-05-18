package textio

import (
	"bufio"
	"github.com/apache/beam/sdks/go/pkg/beam"
	"github.com/apache/beam/sdks/go/pkg/beam/transforms/debug"
	"io/ioutil"
	"log"
	"os"
	"path/filepath"
)

// TODO(herohde) 5/1/2017: should godoc for deferred execution be written as if
// it's immediate (as below)? We'll just write "Foo does Bar" instead of "Foo
// inserts a source/sink/transformation into the pipeline that does Bar", say.

// Read reads a local file and returns the lines as a PCollection<string>. The
// newlines are not part of the lines.
func Read(p *beam.Pipeline, filename string) beam.PCollection {
	p = p.Composite("textio.Read")
	return beam.Source(p, readFn, beam.Data{Data: filename})
}

type fileOpt struct {
	Filename string `beam:"opt"`
}

func readFn(opt fileOpt, emit func(string)) error {
	log.Printf("Reading from %v", opt.Filename)

	file, err := os.Open(opt.Filename)
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

	// TODO(herohde) 4/28/2017: Write needs bundle hook. Hack as side input for now.
	beam.Sink(p, writeFn, debug.Tick(p), beam.SideInput{Input: col}, beam.Data{Data: filename})
}

func writeFn(opt fileOpt, _ string, lines func(*string) bool) error {
	if err := os.MkdirAll(filepath.Dir(opt.Filename), 0755); err != nil {
		return err
	}
	fd, err := ioutil.TempFile(filepath.Dir(opt.Filename), filepath.Base(opt.Filename))
	if err != nil {
		return err
	}
	log.Printf("Writing to %v", fd.Name())

	defer fd.Close()
	writer := bufio.NewWriterSize(fd, 1<<20)

	var line string
	for lines(&line) {
		if _, err := writer.WriteString(line); err != nil {
			return err
		}
		if _, err := writer.Write([]byte{'\n'}); err != nil {
			return err
		}
	}
	writer.Flush()
	return nil
}

// Immediate reads a local file at pipeline construction-time and embeds the
// data into a I/O-free pipeline source. Should be used for small files only.
func Immediate(p *beam.Pipeline, filename string) (beam.PCollection, error) {
	p = p.Composite("textio.Immediate")

	var data []string

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
	return beam.Source(p, linesFn, beam.Data{Data: data}), nil
}

type linesOpt struct {
	Lines []string `beam:"opt"`
}

func linesFn(opt linesOpt, emit func(string)) {
	for _, line := range opt.Lines {
		emit(line)
	}
}
