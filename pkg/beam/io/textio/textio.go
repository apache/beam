package textio

import (
	"bufio"
	"github.com/apache/beam/sdks/go/pkg/beam"
	"io/ioutil"
	"log"
	"os"
	"path/filepath"
)

// TODO(herohde): solve immediate values problem and actually implement. The below
// is cheating, because we use a closure to capture the filename.

func Read(p *beam.Pipeline, filename string) (beam.PCollection, error) {
	return beam.Source(p, func(out chan<- string) error {
		return readFn(filename, out)
	})
}

func readFn(filename string, out chan<- string) error {
	file, err := os.Open(filename)
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
	return beam.Sink(p, func(in <-chan string) error {
		return writeFn(filename, in)
	}, col)
}

func writeFn(filename string, lines <-chan string) error {
	fd, err := ioutil.TempFile(filepath.Dir(filename), filepath.Base(filename))
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
