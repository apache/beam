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

func NodeToPCollection(n *graph.Node) PCollection {
	if n == nil {
		panic("tried converting invalid Node")
	}
	c := PCollection{n}
	c.SetCoder(NewCoder(c.Type()))
	return c
}

func mapPCollectionToNode(pMap map[string]PCollection) map[string]*graph.Node {
	if pMap == nil {
		return nil
	}

	nMap := make(map[string]*graph.Node)
	for k, p := range pMap {
		nMap[k] = PCollectionToNode(p)
	}
	return nMap
}

func mapNodeToPCollection(nMap map[string]*graph.Node) map[string]PCollection {
	if nMap == nil {
		return nil
	}

	pMap := make(map[string]PCollection)
	for k, n := range nMap {
		pMap[k] = NodeToPCollection(n)
	}
	return pMap
}
