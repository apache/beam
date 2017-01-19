package textio

import (
	"bufio"
	"github.com/apache/beam/sdks/go/pkg/beam"
	"io/ioutil"
	"log"
	"os"
	"path/filepath"
)

// TODO(herohde): require that options are top-level? Allow multiple named options?

type Context struct {
	Filename string `beam:"data"`
}

func Read(p *beam.Pipeline, filename string) (beam.PCollection, error) {
	return beam.Source(p, readFn, beam.Data{filename})
}

func readFn(ctx Context, out chan<- string) error {
	log.Printf("Reading from %v", ctx.Filename)

	file, err := os.Open(ctx.Filename)
	if err != nil {
		return err
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		out <- scanner.Text()
	}
	return scanner.Err()
}

func Write(p *beam.Pipeline, filename string, col beam.PCollection) error {
	return beam.Sink(p, writeFn, col, beam.Data{filename})
}

func writeFn(ctx Context, lines <-chan string) error {
	fd, err := ioutil.TempFile(filepath.Dir(ctx.Filename), filepath.Base(ctx.Filename))
	if err != nil {
		return err
	}
	log.Printf("Writing to %v", fd.Name())

	defer fd.Close()
	writer := bufio.NewWriterSize(fd, 1<<20)

	for line := range lines {
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
