package ioutilx

import (
	"errors"
	"io"
)

// ReadN reads exactly N bytes from the reader. Fails otherwise.
func ReadN(r io.Reader, n int) ([]byte, error) {
	ret := make([]byte, n)
	index := 0

	for {
		i, err := r.Read(ret[index:])
		if i+index == n {
			return ret, nil
		}
		if err != nil {
			return nil, err
		}
		if i == 0 {
			return nil, errors.New("No data")
		}

		index += i
	}
}
