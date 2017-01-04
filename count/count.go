package count

import "beam"

// KV uses []byte as a generic "untyped" value with coder-equality.
type KV struct {
	Key   []byte `beam:"key"`
	Count int    `beam:"value"`
}

// GOOD: it is generic.
// NEUTRAL: next transformation needs to know the input type.

// TODO: a real implementation would be less naive ..

func Map(elm []byte) KV {
	return KV{elm, 1}
}

// TODO: decide how much wrangling we can (and want to) allow for KV inputs. We need to
// ensure that we can classify the signature correctly. For example, does the below match
// a []byte collection with a side-input or a KV collection after GBK.

func Reduce(key []byte, counts <-chan int) KV {
	total := 0
	for c := range counts {
		total += c
	}
	return KV{key, total}
}

// PerElement counts the number of elements in a collection by key.
func PerElement(p *beam.Pipeline, col *beam.PCollection) (*beam.PCollection, error) {
	pre, err := beam.ParDo(p, Map, col)
	if err != nil {
		return nil, err
	}
	post, err := beam.GBK(p, pre)
	if err != nil {
		return nil, err
	}
	return beam.ParDo(p, Reduce, post)
}

// TODO: figure out package structure and naming for transforms in the
// standard library.

func Drop(key []byte, counts <-chan int) []byte {
	return key
}

// Dedup removes all duplicates from a collection, under coder equality.
func Dedup(p *beam.Pipeline, col *beam.PCollection) (*beam.PCollection, error) {
	pre, err := beam.ParDo(p, Map, col)
	if err != nil {
		return nil, err
	}
	post, err := beam.GBK(p, pre)
	if err != nil {
		return nil, err
	}
	return beam.ParDo(p, Drop, post)
}
