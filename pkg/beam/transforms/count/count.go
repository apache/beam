package count

import "github.com/apache/beam/sdks/go/pkg/beam"

// NOTE(herohde): KV uses []byte as a generic "untyped" value with coder-equality.

type KV struct {
	Key   []byte `beam:"key"`
	Count int    `beam:"value"`
}

// GOOD: it is generic.
// NEUTRAL: next transformation needs to know the input type.

// TODO: a real implementation would be less naive ..

func Map(elms <-chan []byte, out chan<- KV) {
	for elm := range elms {
		out <- KV{elm, 1}
	}
}

// TODO: decide how much wrangling we can (and want to) allow for KV inputs. We need to
// ensure that we can classify the signature correctly. For example, does the below match
// a []byte collection with a side-input or a KV collection after GBK.

func Reduce(key []byte, counts <-chan int, out chan<- KV) {
	total := 0
	for c := range counts {
		total += c
	}
	out <- KV{key, total}
}

// PerElement counts the number of elements in a collection by key.
func PerElement(p *beam.Pipeline, col beam.PCollection) beam.PCollection {
	p = p.Composite("count.PerElement")

	pre := beam.ParDo(p, Map, col)
	post := beam.GroupByKey(p, pre)
	return beam.ParDo(p, Reduce, post)
}

func Drop(kvs <-chan KV, out chan<- []byte) {
	for kv := range kvs {
		out <- kv.Key
	}
}

// Dedup removes all duplicates from a collection, under coder equality.
func Dedup(p *beam.Pipeline, col beam.PCollection) beam.PCollection {
	p = p.Composite("count.DeDup")

	pre := beam.ParDo(p, Map, col)
	post := beam.GroupByKey(p, pre)
	return beam.ParDo(p, Drop, post)
}
