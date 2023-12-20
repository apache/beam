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

package exec

import (
	"bytes"
	"context"
	"fmt"
	"math"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	"github.com/apache/beam/sdks/v2/go/pkg/beam/core/graph/coder"
)

// PCollection is a passthrough node to collect PCollection metrics, and
// must be placed as the Out node of any producer of a PCollection.
//
// In particular, must not be placed after a Multiplex, and must be placed
// after a Flatten.
type PCollection struct {
	UID         UnitID
	PColID      string
	Out         Node // Out is the consumer of this PCollection.
	Coder       *coder.Coder
	WindowCoder *coder.WindowCoder
	Seed        int64

	r             *rand.Rand
	nextSampleIdx int64 // The index of the next value to sample.
	elementCoder  ElementEncoder
	windowCoder   WindowEncoder

	bundleElementCount                   int64 // must use atomic operations.
	pCollectionElementCount              int64 // track the total number of elements this instance has processed. Local use only, no concurrent read/write.
	sizeMu                               sync.Mutex
	sizeCount, sizeSum, sizeMin, sizeMax int64
	dataSampler                          *DataSampler
}

// ID returns the debug id for this unit.
func (p *PCollection) ID() UnitID {
	return p.UID
}

// Up initializes the random sampling source and element encoder.
func (p *PCollection) Up(ctx context.Context) error {
	// dedicated rand source
	p.r = rand.New(rand.NewSource(p.Seed))
	p.elementCoder = MakeElementEncoder(p.Coder)
	p.windowCoder = MakeWindowEncoder(p.WindowCoder)
	return nil
}

// StartBundle resets collected metrics for this PCollection, and propagates bundle start.
func (p *PCollection) StartBundle(ctx context.Context, id string, data DataContext) error {
	atomic.StoreInt64(&p.bundleElementCount, 0)
	p.nextSampleIdx = 1
	p.resetSize()
	return MultiStartBundle(ctx, id, data, p.Out)
}

type byteCounter struct {
	count int
}

func (w *byteCounter) Write(p []byte) (n int, err error) {
	w.count += len(p)
	return len(p), nil
}

// ProcessElement increments the element count and sometimes takes size samples of the elements.
func (p *PCollection) ProcessElement(ctx context.Context, elm *FullValue, values ...ReStream) error {
	cur := atomic.AddInt64(&p.bundleElementCount, 1)
	if cur+p.pCollectionElementCount == p.nextSampleIdx {
		// Always encode the first 3 elements. Otherwise...
		// We pick the next sampling index based on how large this pcollection already is.
		// We don't want to necessarily wait until the pcollection has doubled, so we reduce the range.
		// We don't want to always encode the first consecutive elements, so we add 2 to give some variance.
		// Finally we add 1 no matter what, so that it can trigger again.
		// Otherwise, there's the potential for the random int to be 0, which means we don't change the
		// nextSampleIdx at all.
		if p.nextSampleIdx < 4 {
			p.nextSampleIdx++
		} else {
			p.nextSampleIdx = cur + p.r.Int63n((cur+p.pCollectionElementCount)/10+2) + 1
		}

		if p.dataSampler == nil {
			var w byteCounter
			p.elementCoder.Encode(elm, &w)
			p.addSize(int64(w.count))
		} else {
			var buf bytes.Buffer
			EncodeWindowedValueHeader(p.windowCoder, elm.Windows, elm.Timestamp, elm.Pane, &buf)
			winSize := buf.Len()
			p.elementCoder.Encode(elm, &buf)
			p.addSize(int64(buf.Len() - winSize))
			p.dataSampler.SendSample(p.PColID, buf.Bytes(), time.Now())
		}
	}
	return p.Out.ProcessElement(ctx, elm, values...)
}

func (p *PCollection) addSize(size int64) {
	p.sizeMu.Lock()
	defer p.sizeMu.Unlock()
	p.sizeCount++
	p.sizeSum += size
	if size > p.sizeMax {
		p.sizeMax = size
	}
	if size < p.sizeMin {
		p.sizeMin = size
	}
}

func (p *PCollection) resetSize() {
	p.sizeMu.Lock()
	defer p.sizeMu.Unlock()
	p.sizeCount = 0
	p.sizeSum = 0
	p.sizeMax = math.MinInt64
	p.sizeMin = math.MaxInt64
}

// FinishBundle propagates bundle termination.
func (p *PCollection) FinishBundle(ctx context.Context) error {
	p.pCollectionElementCount += atomic.LoadInt64(&p.bundleElementCount)
	return MultiFinishBundle(ctx, p.Out)
}

// Down is a no-op.
func (p *PCollection) Down(ctx context.Context) error {
	return nil
}

func (p *PCollection) String() string {
	return fmt.Sprintf("PCollection[%v] Out:%v", p.PColID, IDs(p.Out))
}

// PCollectionSnapshot contains the PCollectionID
type PCollectionSnapshot struct {
	ID           string
	ElementCount int64
	// If SizeCount is zero, then no size metrics should be exported.
	SizeCount, SizeSum, SizeMin, SizeMax int64
}

func (p *PCollection) snapshot() PCollectionSnapshot {
	p.sizeMu.Lock()
	defer p.sizeMu.Unlock()
	return PCollectionSnapshot{
		ID:           p.PColID,
		ElementCount: atomic.LoadInt64(&p.bundleElementCount),
		SizeCount:    p.sizeCount,
		SizeSum:      p.sizeSum,
		SizeMin:      p.sizeMin,
		SizeMax:      p.sizeMax,
	}
}
