// Licensed to the Apache Software Foundation (ASF) under one or more
// contributor license agreements.  See the NOTICE file distributed with
// this work for additional information regarding copyright ownership.
// The ASF licenses this file to You under the Apache License, Version 2.0
// (the "License"); you may not use this file except in compliance with
// the License.  You may obtain a copy of the License at
//
//    http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package stats

// Approximate quantiles is implemented based on https://arxiv.org/pdf/1907.00236.pdf.

import (
	"bytes"
	"container/heap"
	"context"
	"encoding/gob"
	"encoding/json"
	"hash/crc32"
	"io"
	"math"
	"reflect"
	"sort"

	"github.com/apache/beam/sdks/v2/go/pkg/beam"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/core/util/reflectx"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/register"
)

func init() {
	compactorsType := reflect.TypeOf((**compactors)(nil)).Elem()
	weightedElementType := reflect.TypeOf((*weightedElement)(nil)).Elem()
	beam.RegisterType(compactorsType)
	beam.RegisterType(weightedElementType)
	beam.RegisterType(reflect.TypeOf((*approximateQuantilesInputFn)(nil)).Elem())
	beam.RegisterType(reflect.TypeOf((*approximateQuantilesMergeOnlyFn)(nil)).Elem())
	beam.RegisterType(reflect.TypeOf((*approximateQuantilesOutputFn)(nil)).Elem())
	beam.RegisterType(reflect.TypeOf((*shardElementsFn)(nil)).Elem())
	beam.RegisterCoder(compactorsType, encodeCompactors, decodeCompactors)
	beam.RegisterCoder(weightedElementType, encodeWeightedElement, decodeWeightedElement)

	register.Function1x2(fixedKey)
	register.Function2x1(makeWeightedElement)
}

// Opts contains settings used to configure how approximate quantiles are computed.
type Opts struct {
	// Controls the memory used and approximation error (difference between the quantile returned and the true quantile.)
	K int
	// Number of quantiles to return. The algorithm will return NumQuantiles - 1 numbers
	NumQuantiles int
	// For extremely large datasets, runners may have issues with out of memory errors or taking too long to finish.
	// If ApproximateQuantiles is failing, you can use this option to tune how the data is sharded internally.
	// This parameter is optional. If unspecified, Beam will compact all elements into a single compactor at once using a single machine.
	// For example, if this is set to [8, 4, 2]: First, elements will be assigned to 8 shards which will run in parallel. Then the intermediate results from those 8 shards will be reassigned to 4 shards and merged in parallel. Then once again to 2 shards. Finally the intermediate results of those two shards will be merged on one machine before returning the final result.
	InternalSharding []int
}

// The paper suggests reducing the size of the lower-level compactors as we grow.
// We reduce the capacity at this rate.
// The paper suggests 1/sqrt(2) is ideal. That's approximately 0.7.
const capacityCoefficient float64 = 0.7

type sortListHeap struct {
	data [][]beam.T
	less reflectx.Func2x1
}

func (s sortListHeap) Len() int           { return len(s.data) }
func (s sortListHeap) Less(i, j int) bool { return s.less.Call2x1(s.data[i][0], s.data[j][0]).(bool) }
func (s sortListHeap) Swap(i, j int)      { s.data[i], s.data[j] = s.data[j], s.data[i] }
func (s *sortListHeap) Push(x any)        { s.data = append(s.data, x.([]beam.T)) }
func (s *sortListHeap) Pop() any {
	var x beam.T
	x, s.data = s.data[len(s.data)-1], s.data[:len(s.data)-1]
	return x
}

// compactor contains elements to be compacted.
type compactor struct {
	// Compaction needs to sort elements before compacting. Thus in practice, we should often have some pre-sorted data.
	// We want to keep it separate so we can sort only the unsorted data and merge the two sorted lists together.
	// If we're only receiving elements of weight 1, only level 0 will ever contain unsorted data and the rest of the levels will always remain sorted.
	// To prevent repeated allocation/copying, we keep multiple sorted lists and then merge them together
	sorted   [][]beam.T
	unsorted []beam.T
	// How many items should be stored in this compactor before it should get compacted.
	// Note that this is not a hard limit.
	// The paper suggests implementing lazy compaction which would allow
	// compactors to temporarily exceed their capacity as long as the total
	// elements in all compactors doesn't exceed the total capacity in all
	// compactors. In other words, compactors can temporarily borrow capacity
	// from each other.
	// In the paper, this is referred to as the variable k_h.
	capacity int
}

// serializedList represents a list of elements serialized to a byte array.
type serializedList struct {
	// Number of elements serialized to elements.
	Count    int
	Elements []byte
}

type compactorAsGob struct {
	Sorted            []serializedList
	Unsorted          serializedList
	EncodedTypeAsJSON []byte
}

func encodeElements(enc beam.ElementEncoder, elements []beam.T) ([]byte, error) {
	var buf bytes.Buffer
	for _, e := range elements {
		if err := enc.Encode(e, &buf); err != nil {
			return nil, err
		}
	}
	return buf.Bytes(), nil
}

func (c *compactor) getElementType() reflect.Type {
	for _, e := range c.sorted {
		for _, e2 := range e {
			return reflect.TypeOf(e2)
		}
	}
	for _, e := range c.unsorted {
		return reflect.TypeOf(e)
	}
	return nil
}

func (c *compactor) MarshalBinary() ([]byte, error) {
	t := c.getElementType()
	var buf bytes.Buffer
	if t == nil {
		enc := gob.NewEncoder(&buf)
		if err := enc.Encode(compactorAsGob{}); err != nil {
			return nil, err
		}
		return buf.Bytes(), nil
	}
	enc := beam.NewElementEncoder(t)
	encodedSorted := make([]serializedList, 0, len(c.sorted))
	for _, sorted := range c.sorted {
		encoded, err := encodeElements(enc, sorted)
		if err != nil {
			return nil, err
		}
		encodedSorted = append(encodedSorted, serializedList{Count: len(sorted), Elements: encoded})
	}
	encodedUnsortedSerialized, err := encodeElements(enc, c.unsorted)
	encodedUnsorted := serializedList{Count: len(c.unsorted), Elements: encodedUnsortedSerialized}
	if err != nil {
		return nil, err
	}
	tAsJSON, err := beam.EncodedType{T: t}.MarshalJSON()
	if err != nil {
		return nil, err
	}
	gobEnc := gob.NewEncoder(&buf)
	if err = gobEnc.Encode(compactorAsGob{
		Sorted:            encodedSorted,
		Unsorted:          encodedUnsorted,
		EncodedTypeAsJSON: tAsJSON,
	}); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

func (s serializedList) decodeElements(dec beam.ElementDecoder) ([]beam.T, error) {
	buf := bytes.NewBuffer(s.Elements)
	ret := make([]beam.T, 0, s.Count)
	for {
		element, err := dec.Decode(buf)
		if err == io.EOF {
			return ret, nil
		} else if err != nil {
			return nil, err
		}
		ret = append(ret, element)
	}
}

func (c *compactor) UnmarshalBinary(data []byte) error {
	var g compactorAsGob
	var err error
	gobDec := gob.NewDecoder(bytes.NewBuffer(data))
	if err = gobDec.Decode(&g); err != nil {
		return err
	}
	if len(g.EncodedTypeAsJSON) == 0 {
		return nil
	}
	var t beam.EncodedType
	if err = json.Unmarshal(g.EncodedTypeAsJSON, &t); err != nil {
		return err
	}
	dec := beam.NewElementDecoder(t.T)
	decodedSorted := make([][]beam.T, 0, len(g.Sorted))
	for _, sorted := range g.Sorted {
		decoded, err := sorted.decodeElements(dec)
		if err != nil {
			return err
		}
		decodedSorted = append(decodedSorted, decoded)
	}
	c.sorted = decodedSorted
	if c.unsorted, err = g.Unsorted.decodeElements(dec); err != nil {
		return err
	}
	return nil
}

// update inserts an element into the compactor.
func (c *compactor) update(element beam.T) {
	c.unsorted = append(c.unsorted, element)
}

// size returns the number of elements stored in this compactor.
func (c *compactor) size() int {
	size := 0
	for _, s := range c.sorted {
		size += len(s)
	}
	return len(c.unsorted) + size
}

type sorter struct {
	less reflectx.Func2x1
	data []beam.T
}

func (s sorter) Len() int           { return len(s.data) }
func (s sorter) Less(i, j int) bool { return s.less.Call2x1(s.data[i], s.data[j]).(bool) }
func (s sorter) Swap(i, j int)      { s.data[i], s.data[j] = s.data[j], s.data[i] }

// sort sorts the compactor and returns all the elements in sorted order.
func (c *compactor) sort(less reflectx.Func2x1) []beam.T {
	sort.Sort(sorter{data: c.unsorted, less: less})
	h := sortListHeap{data: c.sorted, less: less}
	heap.Init(&h)
	sorted := make([]beam.T, 0, c.size()-len(c.unsorted))
	for h.Len() > 0 {
		s := heap.Pop(&h).([]beam.T)
		sorted = append(sorted, s[0])
		if len(s) > 1 {
			heap.Push(&h, s[1:])
		}
	}
	c.sorted = [][]beam.T{mergeSorted(sorted, c.unsorted, func(a, b any) bool { return less.Call2x1(a, b).(bool) })}
	c.unsorted = nil
	if len(c.sorted[0]) == 0 {
		c.sorted = nil
		return nil
	}
	return c.sorted[0]
}

// Compactors holds the state of the quantile approximation compactors.
type compactors struct {
	// References "K" from the paper which influences the amount of memory used.
	K int
	// When compacting, we want to alternate between taking elements at even vs odd indices.
	// The paper suggests using a random variable but we'd prefer to stay deterministic.
	// Especially when merging two compactors we want to keep track of how often we've selected odds vs evens.
	NumberOfCompactions int

	// Each compactor takes a sample of elements.
	// The "height" (also known as the index in this slice) of the compactor determines the weight of its elements.
	// The weight of a compactor of height h is 2^h.
	// For example, for h = 3 (which would be compactors[3]), the weight is 2^3 = 8. That means each element in that compactor represents 8 instances of itself.
	Compactors []compactor
}

func (c *compactors) totalCapacity() int {
	totalCapacity := 0
	for _, compactor := range c.Compactors {
		totalCapacity += compactor.capacity
	}
	return totalCapacity
}

func (c *compactors) size() int {
	size := 0
	for _, compactor := range c.Compactors {
		size += compactor.size()
	}
	return size
}

// capacity computes the capacity of a compactor at a certain level.
// The paper suggests decreasing the capacity of lower-leveled compactors as we add more elements.
func (c *compactors) capacity(compactorLevel int) int {
	return int(math.Ceil(math.Pow(capacityCoefficient, float64(len(c.Compactors)-compactorLevel-1))*float64(c.K))) + 1
}

// compact compacts all compactors until the total size is less than the maximum capacity of all compactors.
func (c *compactors) compact(less reflectx.Func2x1) {
	for c.size() > c.totalCapacity() {
		for level, compactor := range c.Compactors {
			if compactor.size() > compactor.capacity {
				c.compactLevel(level, less)
				// Merging compactors can cause us to exceed max capacity in multiple compactors.
				if c.size() < c.totalCapacity() {
					// Do lazy compaction as described in the paper.
					break
				}
			}
		}
	}
}

// update inserts the given element into the compactors. If this element causes the compactors to grow too large, we perform the compaction here.
func (c *compactors) update(element beam.T, weight int, less reflectx.Func2x1) {
	level := int(math.Log2(float64(weight)))
	c.growToIncludeLevel(level)
	c.Compactors[level].update(element)
	// From the paper, we're using the "Splitting the Input" approach.
	remainingWeight := weight - (1 << uint(level))
	// Only attempt compaction if we're doing the last update. Otherwise we'd be compacting too often.
	if remainingWeight <= 0 {
		c.compact(less)
	} else {
		c.update(element, remainingWeight, less)
	}
}

// growToIncludeLevel ensures we have compactors available at the given level.
func (c *compactors) growToIncludeLevel(level int) {
	if len(c.Compactors)-1 >= level {
		return
	}
	for i := len(c.Compactors) - 1; i < level; i++ {
		c.Compactors = append(c.Compactors, compactor{})
	}
	for level := range c.Compactors {
		c.Compactors[level].capacity = c.capacity(level)
	}
}

// compact compacts elements in compactors.
func (c *compactors) compactLevel(level int, less reflectx.Func2x1) {
	c.growToIncludeLevel(level + 1)
	jitterIndex := 0
	// Create a temporary buffer to hold the compacted elements.
	// Buffering the elements like this makes it easier to call mergeSorted.
	compacted := make([]beam.T, 0, c.Compactors[level].size()/2)
	selectEvens := c.NumberOfCompactions%2 == 0
	c.NumberOfCompactions++
	for _, element := range c.Compactors[level].sort(less) {
		if (jitterIndex%2 == 0) == selectEvens {
			compacted = append(compacted, element)
		}
		jitterIndex++
	}
	if len(compacted) > 0 {
		c.Compactors[level+1].sorted = append(c.Compactors[level+1].sorted, compacted)
	}
	// Clear out the compactor at this level since we've finished compacting it. The compacted elements have already been moved to the next compactor.
	c.Compactors[level].sorted = nil
	c.Compactors[level].unsorted = nil
}

func encodeCompactors(c *compactors) ([]byte, error) {
	var buf bytes.Buffer
	enc := gob.NewEncoder(&buf)
	if err := enc.Encode(c); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

func decodeCompactors(data []byte) (*compactors, error) {
	var compactors compactors
	dec := gob.NewDecoder(bytes.NewBuffer(data))
	if err := dec.Decode(&compactors); err != nil {
		return nil, err
	}
	for level := range compactors.Compactors {
		compactors.Compactors[level].capacity = compactors.capacity(level)
	}
	return &compactors, nil
}

// mergeSorted takes two slices which are already sorted and returns a new slice containing all elements sorted together.
func mergeSorted(a, b []beam.T, less func(any, any) bool) []beam.T {
	output := make([]beam.T, 0, len(a)+len(b))
	for len(a) > 0 && len(b) > 0 {
		if less(a[0], b[0]) {
			output = append(output, a[0])
			a = a[1:]
		} else {
			output = append(output, b[0])
			b = b[1:]
		}
	}
	if len(a) > 0 {
		output = append(output, a...)
	} else {
		output = append(output, b...)
	}
	return output
}

// mergeSortedWeighted takes two slices which are already sorted and returns a new slice containing all elements sorted together.
func mergeSortedWeighted(a, b []weightedElement, less func(any, any) bool) []weightedElement {
	output := make([]weightedElement, 0, len(a)+len(b))
	for len(a) > 0 && len(b) > 0 {
		if less(a[0], b[0]) {
			output = append(output, a[0])
			a = a[1:]
		} else {
			output = append(output, b[0])
			b = b[1:]
		}
	}
	if len(a) > 0 {
		output = append(output, a...)
	} else {
		output = append(output, b...)
	}
	return output
}

// merge joins two compactors together.
func (c *compactors) merge(other *compactors, less reflectx.Func2x1) {
	for level := range c.Compactors {
		if len(other.Compactors)-1 < level {
			break
		}
		c.Compactors[level].unsorted = append(c.Compactors[level].unsorted, other.Compactors[level].unsorted...)
		c.Compactors[level].sorted = append(c.Compactors[level].sorted, other.Compactors[level].sorted...)
	}
	if len(other.Compactors) > len(c.Compactors) {
		c.Compactors = append(c.Compactors, other.Compactors[len(c.Compactors):]...)
	}
	c.NumberOfCompactions += other.NumberOfCompactions
	c.compact(less)
}

// approximateQuantilesCombineFnState contains the payload for the combiners.
// Ideally this would be a single combine function but if we do that, runners attempt to do all the merges on a single machine.
// Unfortunately the merges can be slow for extremely large datasets and large values of K. If the merge takes too long, it will get canceled and the job will never complete.
// Thus we split up the combiners into multiple functions to force the runner to do the work in parallel.
// This state can be shared across all of the split-up functions.
type approximateQuantilesCombineFnState struct {
	// The size of the compactors.
	// The memory consumed, and the error are controlled by this parameter.
	K int `json:"k"`
	// Used to compare elements.
	LessFunc beam.EncodedFunc
	// Internally cached instance.
	less         reflectx.Func2x1
	NumQuantiles int `json:"numQuantiles"`
}

func (f *approximateQuantilesCombineFnState) setup() error {
	f.less = reflectx.ToFunc2x1(f.LessFunc.Fn)
	return nil
}

func (f *approximateQuantilesCombineFnState) createAccumulator() *compactors {
	return &compactors{
		K:          f.K,
		Compactors: []compactor{{capacity: f.K}},
	}
}

// approximateQuantilesOutputFn extracts the final output containing the quantiles.
type approximateQuantilesOutputFn struct {
	State approximateQuantilesCombineFnState `json:"state"`
}

func (f *approximateQuantilesOutputFn) Setup() error {
	return f.State.setup()
}

func (f *approximateQuantilesOutputFn) CreateAccumulator() *compactors {
	return f.State.createAccumulator()
}

func (f *approximateQuantilesOutputFn) AddInput(compactors *compactors, element *compactors) *compactors {
	compactors.merge(element, f.State.less)
	return compactors
}

func (f *approximateQuantilesOutputFn) MergeAccumulators(ctx context.Context, a, b *compactors) *compactors {
	a.merge(b, f.State.less)
	return a
}

type weightedElementAsGob struct {
	EncodedTypeAsJSON []byte
	Weight            int
	Element           []byte
}

func encodeWeightedElement(element weightedElement) ([]byte, error) {
	t := reflect.TypeOf(element.element)
	enc := beam.NewElementEncoder(t)
	var buf bytes.Buffer
	if err := enc.Encode(element.element, &buf); err != nil {
		return nil, err
	}
	tAsJSON, err := beam.EncodedType{T: t}.MarshalJSON()
	if err != nil {
		return nil, err
	}
	var gobBuf bytes.Buffer
	if err := gob.NewEncoder(&gobBuf).Encode(weightedElementAsGob{
		EncodedTypeAsJSON: tAsJSON,
		Weight:            element.weight,
		Element:           buf.Bytes(),
	}); err != nil {
		return nil, err
	}
	return gobBuf.Bytes(), nil
}

func decodeWeightedElement(data []byte) (weightedElement, error) {
	var g weightedElementAsGob
	dec := gob.NewDecoder(bytes.NewBuffer(data))
	if err := dec.Decode(&g); err != nil {
		return weightedElement{}, err
	}
	var t beam.EncodedType
	if err := t.UnmarshalJSON(g.EncodedTypeAsJSON); err != nil {
		return weightedElement{}, err
	}
	element, err := beam.NewElementDecoder(t.T).Decode(bytes.NewBuffer(g.Element))
	if err != nil {
		return weightedElement{}, err
	}
	return weightedElement{
		weight:  g.Weight,
		element: element,
	}, nil
}

type weightedElement struct {
	weight  int
	element beam.T
}

func toWeightedSlice(compactor compactor, less reflectx.Func2x1, weight int) []weightedElement {
	sorted := compactor.sort(less)
	weightedElements := make([]weightedElement, 0, len(sorted))
	for _, element := range sorted {
		weightedElements = append(weightedElements, weightedElement{weight: weight, element: element})
	}
	return weightedElements
}
func (f *approximateQuantilesOutputFn) ExtractOutput(ctx context.Context, compactors *compactors) []beam.T {
	sorted := toWeightedSlice(compactors.Compactors[0], f.State.less, 1)
	for level, compactor := range compactors.Compactors[1:] {
		sorted = mergeSortedWeighted(sorted, toWeightedSlice(compactor, f.State.less, 1<<uint(level)), func(a, b any) bool {
			return f.State.less.Call2x1(a.(weightedElement).element, b.(weightedElement).element).(bool)
		})
	}
	totalElements := 0
	for _, element := range sorted {
		totalElements += element.weight
	}
	ret := make([]beam.T, 0, f.State.NumQuantiles)
	// Rank represents the estimate of how many elements we've seen as we iterate through the sorted list of elements stored in the compactors.
	// Recall that each element stored in a compactor is also assigned a weight indicating how many elements from the input it represents.
	rank := float64(0)
	// Represents the quantile we're currently searching for.
	currentQuantile := float64(1)
	for _, element := range sorted {
		rank += float64(element.weight)
		if rank/float64(totalElements) >= currentQuantile/float64(f.State.NumQuantiles) {
			ret = append(ret, element.element)
			currentQuantile++
		}
		if currentQuantile >= float64(f.State.NumQuantiles) {
			break
		}
	}
	return ret
}

// approximateQuantilesInputFn combines elements into compactors, but not necessarily the final compactor.
type approximateQuantilesInputFn approximateQuantilesOutputFn

func (f *approximateQuantilesInputFn) Setup() error {
	return f.State.setup()
}

func (f *approximateQuantilesInputFn) CreateAccumulator() *compactors {
	return f.State.createAccumulator()
}

func (f *approximateQuantilesInputFn) AddInput(compactors *compactors, element weightedElement) *compactors {
	compactors.update(element.element, element.weight, f.State.less)
	return compactors
}

func (f *approximateQuantilesInputFn) MergeAccumulators(ctx context.Context, a, b *compactors) *compactors {
	a.merge(b, f.State.less)
	return a
}

func (f *approximateQuantilesInputFn) ExtractOutput(ctx context.Context, compactors *compactors) *compactors {
	for i := range compactors.Compactors {
		// Sort the compactors here so when we're merging them for the final output, they're already sorted and we can merge elements in order.
		compactors.Compactors[i].sort(f.State.less)
	}
	return compactors
}

// approximateQuantilesMergeOnlyFn combines compactors into smaller compactors, but not necessarily the final compactor.
type approximateQuantilesMergeOnlyFn approximateQuantilesOutputFn

func (f *approximateQuantilesMergeOnlyFn) Setup() error {
	return f.State.setup()
}

func (f *approximateQuantilesMergeOnlyFn) CreateAccumulator() *compactors {
	return f.State.createAccumulator()
}

func (f *approximateQuantilesMergeOnlyFn) AddInput(compactors *compactors, element *compactors) *compactors {
	compactors.merge(element, f.State.less)
	return compactors
}

func (f *approximateQuantilesMergeOnlyFn) MergeAccumulators(ctx context.Context, a, b *compactors) *compactors {
	a.merge(b, f.State.less)
	return a
}

func (f *approximateQuantilesMergeOnlyFn) ExtractOutput(ctx context.Context, compactors *compactors) *compactors {
	for i := range compactors.Compactors {
		// Sort the compactors here so when we're merging them for the final output, they're already sorted and we can merge elements in order.
		compactors.Compactors[i].sort(f.State.less)
	}
	return compactors
}

type shardElementsFn struct {
	Shards         int              `json:"shards"`
	T              beam.EncodedType `json:"t"`
	elementEncoder beam.ElementEncoder
}

func (s *shardElementsFn) Setup() {
	s.elementEncoder = beam.NewElementEncoder(s.T.T)
}

func (s *shardElementsFn) ProcessElement(element beam.T) (int, beam.T) {
	h := crc32.NewIEEE()
	s.elementEncoder.Encode(element, h)
	return int(h.Sum32()) % s.Shards, element
}

func makeWeightedElement(weight int, element beam.T) weightedElement {
	return weightedElement{weight: weight, element: element}
}

func fixedKey(e beam.T) (int, beam.T) { return 1, e }

// ApproximateQuantiles computes approximate quantiles for the input PCollection<T>.
//
// The output PCollection contains a single element: a list of numQuantiles - 1 elements approximately splitting up the input collection into numQuantiles separate quantiles.
// For example, if numQuantiles = 2, the returned list would contain a single element such that approximately half of the input would be less than that element and half would be greater.
func ApproximateQuantiles(s beam.Scope, pc beam.PCollection, less any, opts Opts) beam.PCollection {
	return ApproximateWeightedQuantiles(s, beam.ParDo(s, fixedKey, pc), less, opts)
}

// reduce takes a PCollection<weightedElementWrapper> and returns a PCollection<*compactors>. The output PCollection may have at most shardSizes[len(shardSizes) - 1] compactors.
func reduce(s beam.Scope, weightedElements beam.PCollection, state approximateQuantilesCombineFnState, shardSizes []int) beam.PCollection {
	if len(shardSizes) == 0 {
		shardSizes = []int{1}
	}
	elementsWithShardNumber := beam.ParDo(s, &shardElementsFn{Shards: shardSizes[0], T: beam.EncodedType{T: reflect.TypeOf((*weightedElement)(nil)).Elem()}}, weightedElements)
	reducedCompactorsWithShardNumber := beam.CombinePerKey(s, &approximateQuantilesInputFn{State: state}, elementsWithShardNumber)
	shardedCompactors := beam.DropKey(s, reducedCompactorsWithShardNumber)
	shardSizes = shardSizes[1:]
	compactorsType := reflect.TypeOf((**compactors)(nil)).Elem()
	for _, shardSize := range shardSizes {
		compactorsWithShardNumber := beam.ParDo(s, &shardElementsFn{Shards: shardSize, T: beam.EncodedType{T: compactorsType}}, shardedCompactors)
		reducedCompactorsWithShardNumber = beam.CombinePerKey(s, &approximateQuantilesMergeOnlyFn{State: state}, compactorsWithShardNumber)
		shardedCompactors = beam.DropKey(s, reducedCompactorsWithShardNumber)
	}
	return shardedCompactors
}

// ApproximateWeightedQuantiles computes approximate quantiles for the input PCollection<(weight int, T)>.
//
// The output PCollection contains a single element: a list of numQuantiles - 1 elements approximately splitting up the input collection into numQuantiles separate quantiles.
// For example, if numQuantiles = 2, the returned list would contain a single element such that approximately half of the input would be less than that element and half would be greater or equal.
func ApproximateWeightedQuantiles(s beam.Scope, pc beam.PCollection, less any, opts Opts) beam.PCollection {
	_, t := beam.ValidateKVType(pc)
	state := approximateQuantilesCombineFnState{
		K:            opts.K,
		NumQuantiles: opts.NumQuantiles,
		LessFunc:     beam.EncodedFunc{Fn: reflectx.MakeFunc(less)},
	}
	weightedElements := beam.ParDo(s, makeWeightedElement, pc)
	shardedCompactors := reduce(s, weightedElements, state, opts.InternalSharding)
	return beam.Combine(
		s,
		&approximateQuantilesOutputFn{State: state},
		shardedCompactors,
		beam.TypeDefinition{Var: beam.TType, T: t.Type()},
	)
}
