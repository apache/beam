package beam

import (
	"github.com/apache/beam/sdks/go/pkg/beam/core/graph"
)

func PCollectionToNode(p PCollection) *graph.Node {
	if !p.IsValid() {
		panic("tried converting invalid PCollection")
	}
	return p.n
}

func NodeToPCollection(n *graph.Node) PCollection{
	if n == nil {
		panic("tried converting invalid Node")
	}
	return PCollection {n: n}
}

func mapPCollectionToNode(pMap map[string]PCollection) map[string]*graph.Node {
	nMap := make(map[string]*graph.Node)
	for k, p := range pMap {
		nMap[k] = PCollectionToNode(p)
	}
	return nMap
}

func mapNodeToPCollection (nMap map[string]*graph.Node) pMap map[string]PCollection{
	pMap := make(map[string]PCollection)
	for k, n := range nMap {
		nMap[k] = NodeToPCollection(n)
	}
	return pMap
}