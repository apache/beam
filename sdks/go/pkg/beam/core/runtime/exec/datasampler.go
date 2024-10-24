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
	"context"
	"sync"
	"time"
)

// DataSample contains property for sampled element
type DataSample struct {
	PCollectionID string
	Timestamp     time.Time
	Element       []byte
}

// DataSampler manages sampled elements based on PCollectionID
type DataSampler struct {
	sampleChannel chan *DataSample
	samplesMap    sync.Map // Key: PCollectionID string, Value: *OutputSamples pointer
	ctx           context.Context
}

// NewDataSampler inits a new Data Sampler object and returns pointer to it.
func NewDataSampler(ctx context.Context) *DataSampler {
	return &DataSampler{
		sampleChannel: make(chan *DataSample, 1000),
		ctx:           ctx,
	}
}

// Process processes sampled element.
func (d *DataSampler) Process() {
	for {
		select {
		case <-d.ctx.Done():
			return
		case sample := <-d.sampleChannel:
			d.addSample(sample)
		}
	}
}

// GetSamples returns samples for given pCollectionID.
// If no pCollectionID is provided, return all available samples
func (d *DataSampler) GetSamples(pids []string) map[string][]*DataSample {
	if len(pids) == 0 {
		return d.getAllSamples()
	}
	return d.getSamplesForPCollections(pids)
}

// SendSample is called by PCollection Node to send sampled element to Data Sampler async
func (d *DataSampler) SendSample(pCollectionID string, element []byte, timestamp time.Time) {
	sample := DataSample{
		PCollectionID: pCollectionID,
		Element:       element,
		Timestamp:     timestamp,
	}
	d.sampleChannel <- &sample
}

func (d *DataSampler) getAllSamples() map[string][]*DataSample {
	var res = make(map[string][]*DataSample)
	d.samplesMap.Range(func(key any, value any) bool {
		pid := key.(string)
		samples := d.getSamples(pid)
		if len(samples) > 0 {
			res[pid] = samples
		}
		return true
	})
	return res
}

func (d *DataSampler) getSamplesForPCollections(pids []string) map[string][]*DataSample {
	var res = make(map[string][]*DataSample)
	for _, pid := range pids {
		samples := d.getSamples(pid)
		if len(samples) > 0 {
			res[pid] = samples
		}
	}
	return res
}

func (d *DataSampler) addSample(sample *DataSample) {
	p, ok := d.samplesMap.Load(sample.PCollectionID)
	if !ok {
		p = &outputSamples{maxElements: 10, sampleIndex: 0}
		d.samplesMap.Store(sample.PCollectionID, p)
	}
	outputSamples := p.(*outputSamples)
	outputSamples.addSample(sample)
}

func (d *DataSampler) getSamples(pCollectionID string) []*DataSample {
	p, ok := d.samplesMap.Load(pCollectionID)
	if !ok {
		return nil
	}
	outputSamples := p.(*outputSamples)
	return outputSamples.getSamples()
}

type outputSamples struct {
	elements    []*DataSample
	mu          sync.Mutex
	maxElements int
	sampleIndex int
}

func (o *outputSamples) addSample(element *DataSample) {
	o.mu.Lock()
	defer o.mu.Unlock()

	if len(o.elements) < o.maxElements {
		o.elements = append(o.elements, element)
	} else {
		o.elements[o.sampleIndex] = element
		o.sampleIndex = (o.sampleIndex + 1) % o.maxElements
	}
}

func (o *outputSamples) getSamples() []*DataSample {
	o.mu.Lock()
	defer o.mu.Unlock()
	if len(o.elements) == 0 {
		return nil
	}
	samples := o.elements

	// Reset index and samples
	o.sampleIndex = 0
	// Release memory since samples are only returned once based on best efforts
	o.elements = nil

	return samples
}
