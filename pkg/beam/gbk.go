package beam

import (
	"fmt"
	"github.com/apache/beam/sdks/go/pkg/beam/graph"
)

// GroupByKey inserts a GBK transform into the pipeline.
func GroupByKey(p *Pipeline, a PCollection) PCollection {
	return Must(TryGroupByKey(p, a))
}

// TryGroupByKey inserts a GBK transform into the pipeline. Returns
// an error on failure.
func TryGroupByKey(p *Pipeline, a PCollection) (PCollection, error) {
	if !a.IsValid() {
		return PCollection{}, fmt.Errorf("invalid pcollection to GBK")
	}
	edge, err := graph.NewGBK(p.real, p.parent, a.n)
	if err != nil {
		return PCollection{}, err
	}
	ret := PCollection{edge.Output[0].To}
	ret.SetCoder(NewCoder(ret.Type()))
	return ret, nil
}

// CoGroupByKey inserts a CoGBK transform into the pipeline.
func CoGroupByKey(p *Pipeline, cols ...PCollection) PCollection {
	return Must(TryCoGroupByKey(p, cols...))
}

// TryCoGroupByKey inserts a CoGBK transform into the pipeline. Returns
// an error on failure.
func TryCoGroupByKey(p *Pipeline, cols ...PCollection) (PCollection, error) {
	return PCollection{}, nil
}
