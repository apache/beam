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
	"io"
	"math"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/apache/beam/sdks/v2/go/pkg/beam/core/graph/coder"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/core/sdf"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/core/util/ioutilx"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/internal/errors"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/log"
	"golang.org/x/exp/maps"
)

// DataSource is a Root execution unit.
type DataSource struct {
	UID   UnitID
	SID   StreamID
	Name  string
	Coder *coder.Coder
	Out   Node
	PCol  PCollection // Handles size metrics. Value instead of pointer so it's initialized by default in tests.
	// OnTimerTransforms maps PtransformIDs to their execution nodes that handle OnTimer callbacks.
	OnTimerTransforms map[string]*ParDo

	source  DataManager
	state   StateReader
	curInst string

	index    int64
	splitIdx int64
	start    time.Time

	// su is non-nil if this DataSource feeds directly to a splittable unit,
	// and receives that splittable unit when it is available for splitting.
	// While the splittable unit is received, it is blocked from processing
	// new elements, so it must be sent back through the channel once the
	// DataSource is finished using it.
	su chan SplittableUnit

	mu sync.Mutex

	// Whether the downstream transform only iterates a GBK coder once.
	singleIterate bool

	// represents if the SDK is consuming received data.
	consumingReceivedData atomic.Bool
}

// InitSplittable initializes the SplittableUnit channel from the output unit,
// if it provides one.
func (n *DataSource) InitSplittable() {
	if n.Out == nil {
		return
	}
	if u, ok := n.Out.(*ProcessSizedElementsAndRestrictions); ok {
		n.su = u.SU
	}
}

// ID returns the UnitID for this node.
func (n *DataSource) ID() UnitID {
	return n.UID
}

// Up initializes this datasource.
func (n *DataSource) Up(ctx context.Context) error {
	safeToSingleIterate := true
	switch n.Out.(type) {
	case *Expand, *Multiplex:
		// CoGBK Expands aren't safe, as they may re-iterate the GBK stream.
		// Multiplexes aren't safe, since they re-iterate the GBK stream by default.
		safeToSingleIterate = false
	}
	n.singleIterate = safeToSingleIterate
	return nil
}

// StartBundle initializes this datasource for the bundle.
func (n *DataSource) StartBundle(ctx context.Context, id string, data DataContext) error {
	n.mu.Lock()
	n.curInst = id
	n.source = data.Data
	n.state = data.State
	n.start = time.Now()
	n.index = 0
	n.splitIdx = math.MaxInt64
	n.mu.Unlock()
	return n.Out.StartBundle(ctx, id, data)
}

// process handles converting elements from the data source to timers.
//
// The data and timer callback functions must return an io.EOF if the reader terminates to signal that an additional
// buffer is desired.
func (n *DataSource) process(ctx context.Context, data func(bcr *byteCountReader, ptransformID string) error, timer func(bcr *byteCountReader, ptransformID, timerFamilyID string) error) error {
	// The SID contains this instruction's expected data processing transform (this one).
	elms, err := n.source.OpenElementChan(ctx, n.SID, maps.Keys(n.OnTimerTransforms))
	if err != nil {
		return err
	}

	n.PCol.resetSize() // initialize the size distribution for this bundle.
	var r bytes.Reader

	var byteCount int
	bcr := byteCountReader{reader: &r, count: &byteCount}

	for {
		n.consumingReceivedData.Store(false)
		var err error
		select {
		case e, ok := <-elms:
			n.consumingReceivedData.Store(true)
			// Channel closed, so time to exit
			if !ok {
				return nil
			}
			if len(e.Data) > 0 {
				r.Reset(e.Data)
				err = data(&bcr, e.PtransformID)
			}
			if err != nil && err != io.EOF {
				return errors.Wrapf(err, "source failed processing data")
			}
			// Process any simultaneously sent timers.
			// If the data channel has split though
			if len(e.Timers) > 0 {
				r.Reset(e.Timers)
				err = timer(&bcr, e.PtransformID, e.TimerFamilyID)
			}
			if err != nil && err != io.EOF {
				return errors.Wrap(err, "source failed processing timers")
			}
			// io.EOF means the reader successfully drained.
			// We're ready for a new buffer.
		case <-ctx.Done():
			// now that it is done processing received data, we set it to false.
			n.consumingReceivedData.Store(false)
			return nil
		}
	}
}

// ByteCountReader is a passthrough reader that counts all the bytes read through it.
// It trusts the nested reader to return accurate byte information.
type byteCountReader struct {
	count  *int
	reader io.Reader
}

func (r *byteCountReader) Read(p []byte) (int, error) {
	n, err := r.reader.Read(p)
	*r.count += n
	return n, err
}

func (r *byteCountReader) Close() error {
	if c, ok := r.reader.(io.Closer); ok {
		c.Close()
	}
	return nil
}

func (r *byteCountReader) reset() int {
	c := *r.count
	*r.count = 0
	return c
}

// Process opens the data source, reads and decodes data, kicking off element processing.
func (n *DataSource) Process(ctx context.Context) ([]*Checkpoint, error) {
	c := coder.SkipW(n.Coder)
	wc := MakeWindowDecoder(n.Coder.Window)

	var cp ElementDecoder    // Decoder for the primary element or the key in CoGBKs.
	var cvs []ElementDecoder // Decoders for each value stream in CoGBKs.

	switch {
	case coder.IsCoGBK(c):
		cp = MakeElementDecoder(c.Components[0])

		// TODO(https://github.com/apache/beam/issues/18032): Support multiple value streams (coder components) with
		// with CoGBK.
		cvs = []ElementDecoder{MakeElementDecoder(c.Components[1])}
	default:
		cp = MakeElementDecoder(c)
	}

	hasSplit := map[string]bool{}
	var checkpoints []*Checkpoint
	err := n.process(ctx, func(bcr *byteCountReader, ptransformID string) error {
		// Check if this transform has already successfully, and if so, skip reading and decoding of the elements in the buffer.
		if hasSplit[ptransformID] {
			return nil
		}
		for {
			// TODO(lostluck) 2020/02/22: Should we include window headers or just count the element sizes?
			ws, t, pn, err := DecodeWindowedValueHeader(wc, bcr.reader)
			if err != nil {
				return err
			}

			// Decode key or parallel element.
			pe, err := cp.Decode(bcr)
			if err != nil {
				return errors.Wrap(err, "source decode failed")
			}
			pe.Timestamp = t
			pe.Windows = ws
			pe.Pane = pn

			var valReStreams []ReStream
			for _, cv := range cvs {
				values, err := n.makeReStream(ctx, cv, bcr, len(cvs) == 1 && n.singleIterate)
				if err != nil {
					return err
				}
				valReStreams = append(valReStreams, values)
			}

			if err := n.Out.ProcessElement(ctx, pe, valReStreams...); err != nil {
				return err
			}
			// Collect the actual size of the element, and reset the bytecounter reader.
			// TODO(zechenj18) 2023-12-07: currently we never sample anything from the DataSource, we need to validate CoGBKs and similar types with the sampling implementation
			n.PCol.addSize(int64(bcr.reset()))

			// Check if there's a continuation and return residuals
			// Needs to be done immediately after processing to not lose the element.
			if c := n.getProcessContinuation(); c != nil {
				cp, err := n.checkpointThis(ctx, c)
				if err != nil {
					// Errors during checkpointing should fail a bundle.
					return err
				}
				if cp != nil {
					checkpoints = append(checkpoints, cp)
				}
			}
			//	We've finished processing an element, check if we have finished a split.
			if n.incrementIndexAndCheckSplit() {
				hasSplit[ptransformID] = true
				return nil
			}
		}
	},
		func(bcr *byteCountReader, ptransformID, timerFamilyID string) error {
			if node, ok := n.OnTimerTransforms[ptransformID]; ok {
				if err := node.ProcessTimers(timerFamilyID, bcr); err != nil {
					log.Warnf(ctx, "expected transform %v to have an OnTimer method attached to handle"+
						"Timer Family ID: %v callback, but it did not. Please file an issue with Apache Beam"+
						"if you have defined OnTimer method with reproducible code at https://github.com/apache/beam/issues", ptransformID, timerFamilyID)
					return errors.WithContext(err, "ontimer callback invocation failed")
				}
			}
			return nil
		})

	return checkpoints, err
}

func (n *DataSource) makeReStream(ctx context.Context, cv ElementDecoder, bcr *byteCountReader, onlyStream bool) (ReStream, error) {
	// TODO(lostluck) 2020/02/22: Do we include the chunk size, or just the element sizes?
	size, err := coder.DecodeInt32(bcr.reader)
	if err != nil {
		return nil, errors.Wrap(err, "stream size decoding failed")
	}

	if onlyStream {
		// If we know the stream won't be re-iterated,
		// decode elements on demand instead to reduce memory usage.
		switch {
		case size >= 0:
			return &singleUseReStream{
				r:    bcr,
				d:    cv,
				size: int(size),
			}, nil
		case size == -1:
			return &singleUseMultiChunkReStream{
				r: bcr,
				d: cv,
				open: func(bcr *byteCountReader) (Stream, error) {
					tokenLen, err := coder.DecodeVarInt(bcr.reader)
					if err != nil {
						return nil, err
					}
					token, err := ioutilx.ReadN(bcr.reader, (int)(tokenLen))
					if err != nil {
						return nil, err
					}
					r, err := n.state.OpenIterable(ctx, n.SID, token)
					if err != nil {
						return nil, err
					}
					// We can't re-use the original bcr, since we may get new iterables,
					// but we can re-use the count itself.
					r = &byteCountReader{reader: r, count: bcr.count}
					return &elementStream{r: r, ec: cv}, nil
				},
			}, nil
		}
	}

	switch {
	case size >= 0:
		// Single chunk streams are fully read in and buffered in memory.
		buf := make([]FullValue, 0, size)
		buf, err = readStreamToBuffer(cv, bcr, int64(size), buf)
		if err != nil {
			return nil, err
		}
		return &FixedReStream{Buf: buf}, nil
	case size == -1:
		// Multi-chunked stream.
		var buf []FullValue
		for {
			chunk, err := coder.DecodeVarInt(bcr.reader)
			if err != nil {
				return nil, errors.Wrap(err, "stream chunk size decoding failed")
			}
			// All done, escape out.
			switch {
			case chunk == 0: // End of stream, return buffer.
				return &FixedReStream{Buf: buf}, nil
			case chunk > 0: // Non-zero chunk, read that many elements from the stream, and buffer them.
				chunkBuf := make([]FullValue, 0, chunk)
				chunkBuf, err = readStreamToBuffer(cv, bcr, chunk, chunkBuf)
				if err != nil {
					return nil, err
				}
				buf = append(buf, chunkBuf...)
			case chunk == -1: // State backed iterable!
				chunk, err := coder.DecodeVarInt(bcr.reader)
				if err != nil {
					return nil, err
				}
				token, err := ioutilx.ReadN(bcr.reader, (int)(chunk))
				if err != nil {
					return nil, err
				}
				return &concatReStream{
					first: &FixedReStream{Buf: buf},
					next: &proxyReStream{
						open: func() (Stream, error) {
							r, err := n.state.OpenIterable(ctx, n.SID, token)
							if err != nil {
								return nil, err
							}
							// We can't re-use the original bcr, since we may get new iterables,
							// or multiple of them at the same time, but we can re-use the count itself.
							r = &byteCountReader{reader: r, count: bcr.count}
							return &elementStream{r: r, ec: cv}, nil
						},
					},
				}, nil
			default:
				return nil, errors.Errorf("multi-chunk stream with invalid chunk size of %d", chunk)
			}
		}
	default:
		return nil, errors.Errorf("received stream with marker size of %d", size)
	}
}

func readStreamToBuffer(cv ElementDecoder, r io.Reader, size int64, buf []FullValue) ([]FullValue, error) {
	for i := int64(0); i < size; i++ {
		value, err := cv.Decode(r)
		if err != nil {
			return nil, errors.Wrap(err, "stream value decode failed")
		}
		buf = append(buf, *value)
	}
	return buf, nil
}

// FinishBundle resets the source.
func (n *DataSource) FinishBundle(ctx context.Context) error {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.source = nil
	n.splitIdx = 0 // Ensure errors are returned for split requests if this plan is re-used.
	return n.Out.FinishBundle(ctx)
}

// Down resets the source.
func (n *DataSource) Down(ctx context.Context) error {
	n.source = nil
	return nil
}

func (n *DataSource) String() string {
	return fmt.Sprintf("DataSource[%v, %v] Out:%v Coder:%v ", n.SID, n.Name, n.Out.ID(), n.Coder)
}

// incrementIndexAndCheckSplit increments DataSource.index by one and checks if
// the caller should abort further element processing, and finish the bundle.
// Returns true if the new value of index is greater than or equal to the split
// index, and false otherwise.
func (n *DataSource) incrementIndexAndCheckSplit() bool {
	b := false
	n.mu.Lock()
	n.index++
	if n.index >= n.splitIdx {
		b = true
	}
	n.mu.Unlock()
	return b
}

// ProgressReportSnapshot captures the progress reading an input source.
type ProgressReportSnapshot struct {
	ID, Name string
	Count    int64

	pcol                  PCollectionSnapshot
	ConsumingReceivedData bool
}

// Progress returns a snapshot of the source's progress.
func (n *DataSource) Progress() ProgressReportSnapshot {
	if n == nil {
		return ProgressReportSnapshot{}
	}
	n.mu.Lock()
	pcol := n.PCol.snapshot()
	// The count is the number of "completely processed elements"
	// which matches the index of the currently processing element.
	c := n.index
	// Retrieve the signal from the Data source.
	consumingReceivedData := n.consumingReceivedData.Load()
	n.mu.Unlock()
	// Do not sent negative progress reports, index is initialized to 0.
	if c < 0 {
		c = 0
	}
	pcol.ElementCount = c
	return ProgressReportSnapshot{ID: n.SID.PtransformID, Name: n.Name, Count: c, pcol: pcol, ConsumingReceivedData: consumingReceivedData}
}

// getProcessContinuation retrieves a ProcessContinuation that may be returned by
// a self-checkpointing SDF. Current support for self-checkpointing requires that the
// SDF is immediately after the DataSource.
func (n *DataSource) getProcessContinuation() sdf.ProcessContinuation {
	if u, ok := n.Out.(*ProcessSizedElementsAndRestrictions); ok {
		return u.continuation
	}
	return nil
}

func (n *DataSource) makeEncodeElms() func([]*FullValue) ([][]byte, error) {
	wc := MakeWindowEncoder(n.Coder.Window)
	ec := MakeElementEncoder(coder.SkipW(n.Coder))
	encodeElms := func(fvs []*FullValue) ([][]byte, error) {
		encElms := make([][]byte, len(fvs))
		for i, fv := range fvs {
			enc, err := encodeElm(fv, wc, ec)
			if err != nil {
				return nil, err
			}
			encElms[i] = enc
		}
		return encElms, nil
	}
	return encodeElms
}

type Checkpoint struct {
	SR      SplitResult
	Reapply time.Duration
}

// Checkpoint attempts to split an SDF that has self-checkpointed (e.g. returned a
// ProcessContinuation) and needs to be resumed later. If the underlying DoFn is not
// splittable or has not returned a resuming continuation, the function returns an empty
// SplitResult, a negative resumption time, and a false boolean to indicate that no split
// occurred.
func (n *DataSource) checkpointThis(ctx context.Context, pc sdf.ProcessContinuation) (*Checkpoint, error) {
	n.mu.Lock()
	defer n.mu.Unlock()

	if pc == nil || !pc.ShouldResume() {
		return nil, nil
	}

	su := SplittableUnit(n.Out.(*ProcessSizedElementsAndRestrictions))

	ow := su.GetOutputWatermark()

	// Checkpointing is functionally a split at fraction 0.0
	rs, err := su.Checkpoint(ctx)
	if err != nil {
		return nil, err
	}
	if len(rs) == 0 {
		return nil, nil
	}

	encodeElms := n.makeEncodeElms()

	rsEnc, err := encodeElms(rs)
	if err != nil {
		return nil, err
	}

	res := SplitResult{
		RS:   rsEnc,
		TId:  su.GetTransformId(),
		InId: su.GetInputId(),
		OW:   ow,
	}
	return &Checkpoint{SR: res, Reapply: pc.ResumeDelay()}, nil
}

// Split takes a sorted set of potential split indices and a fraction of the
// remainder to split at, selects and actuates a split on an appropriate split
// index, and returns the selected split index in a SplitResult if successful or
// an error when unsuccessful.
//
// If the following transform is splittable, and the split indices and fraction
// allow for splitting on the currently processing element, then a sub-element
// split is performed, and the appropriate information is returned in the
// SplitResult.
//
// The bufSize param specifies the estimated number of elements that will be
// sent to this DataSource, and is used to be able to perform accurate splits
// even if the DataSource has not yet received all its elements. A bufSize of
// 0 or less indicates that it's unknown, and so uses the current known size.
func (n *DataSource) Split(ctx context.Context, splits []int64, frac float64, bufSize int64) (SplitResult, error) {
	if n == nil {
		return SplitResult{}, fmt.Errorf("failed to split at requested splits: {%v}, DataSource not initialized", splits)
	}
	if frac > 1.0 {
		frac = 1.0
	} else if frac < 0.0 {
		frac = 0.0
	}

	n.mu.Lock()
	defer n.mu.Unlock()

	var currProg float64 // Current element progress.
	var su SplittableUnit
	if n.index < 0 { // Progress is at the end of the non-existant -1st element.
		currProg = 1.0
	} else if n.su == nil { // If this isn't sub-element splittable, estimate some progress.
		currProg = 0.5
	} else { // If this is sub-element splittable, get progress of the current element.

		select {
		case su = <-n.su:
			// If an element is processing, we'll get a splittable unit.
			if su == nil {
				return SplitResult{}, fmt.Errorf("failed to split: splittable unit was nil")
			}
			defer func() {
				n.su <- su
			}()
			currProg = su.GetProgress()
		case <-time.After(500 * time.Millisecond):
			// Otherwise, the current element hasn't started processing yet
			// or has already finished. By adding a short timeout, we avoid
			// the first possibility, and can assume progress is at max.
			currProg = 1.0
		}
	}
	// Size to split within is the minimum of bufSize or splitIdx so we avoid
	// including elements we already know won't be processed.
	if bufSize <= 0 || n.splitIdx < bufSize {
		bufSize = n.splitIdx
	}
	s, fr, err := splitHelper(n.index, bufSize, currProg, splits, frac, su != nil)
	if err != nil {
		log.Infof(ctx, "Unsuccessful split: %v", err)
		return SplitResult{Unsuccessful: true}, nil
	}

	// No fraction returned, perform channel split.
	if fr < 0 {
		n.splitIdx = s
		return SplitResult{PI: s - 1, RI: s}, nil
	}
	// Get the output watermark before splitting to avoid accidentally overestimating
	ow := su.GetOutputWatermark()
	// Otherwise, perform a sub-element split.
	ps, rs, err := su.Split(ctx, fr)
	if err != nil {
		return SplitResult{}, err
	}

	if len(ps) == 0 || len(rs) == 0 { // Unsuccessful split.
		// Fallback to channel split, so split at next elm, not current.
		n.splitIdx = s + 1
		return SplitResult{PI: s, RI: s + 1}, nil
	}

	// TODO(https://github.com/apache/beam/issues/20343) Eventually encode elements with the splittable
	// unit's input coder instead of the DataSource's coder.
	encodeElms := n.makeEncodeElms()

	psEnc, err := encodeElms(ps)
	if err != nil {
		return SplitResult{}, err
	}
	rsEnc, err := encodeElms(rs)
	if err != nil {
		return SplitResult{}, err
	}
	n.splitIdx = s + 1 // In a sub-element split, s is currIdx.
	res := SplitResult{
		PI:   s - 1,
		RI:   s + 1,
		PS:   psEnc,
		RS:   rsEnc,
		TId:  su.GetTransformId(),
		InId: su.GetInputId(),
		OW:   ow,
	}
	return res, nil
}

// splitHelper is a helper function that finds a split point in a range.
//
// currIdx and endIdx should match the DataSource's index and splitIdx fields,
// and represent the start and end of the splittable range respectively.
//
// currProg represents the progress through the current element (currIdx).
//
// splits is an optional slice of valid split indices, and if nil then all
// indices are considered valid split points.
//
// frac must be between [0, 1], and represents a fraction of the remaining work
// that the split point aims to be as close as possible to.
//
// splittable indicates that sub-element splitting is possible (i.e. the next
// unit is an SDF).
//
// Returns the element index to split at (first element of residual). If the
// split position qualifies for sub-element splitting, then this also returns
// the fraction of remaining work in the current element to use as a split
// fraction for a sub-element split, and otherwise returns -1.
//
// A split point is sub-element splittable iff the split point is the current
// element, the splittable param is set to true, and both the element being
// split and the following element are valid split points.
func splitHelper(
	currIdx, endIdx int64,
	currProg float64,
	splits []int64,
	frac float64,
	splittable bool) (int64, float64, error) {
	// Get split index from fraction. Find the closest index to the fraction of
	// the remainder.
	start := float64(currIdx) + currProg
	safeStart := currIdx + 1 // safeStart avoids splitting at 0, or <= currIdx
	if safeStart <= 0 {
		safeStart = 1
	}
	var splitFloat = start + frac*(float64(endIdx)-start)

	// Handle simpler cases where all split points are valid first.
	if len(splits) == 0 {
		if splittable && int64(splitFloat) == currIdx {
			// Sub-element splitting is valid.
			_, f := math.Modf(splitFloat)
			// Convert from fraction of entire element to fraction of remainder.
			fr := (f - currProg) / (1.0 - currProg)
			return int64(splitFloat), fr, nil
		}
		// All split points are valid so just split at safe index closest to
		// fraction.
		splitIdx := int64(math.Round(splitFloat))
		if splitIdx < safeStart {
			splitIdx = safeStart
		}
		return splitIdx, -1.0, nil
	}

	// Cases where we have to find a valid split point.
	sort.Slice(splits, func(i, j int) bool { return splits[i] < splits[j] })
	if splittable && int64(splitFloat) == currIdx {
		// Check valid split points to see if we can do a sub-element split.
		// We need to find the currIdx and currIdx + 1 for it to be valid.
		c, cp1 := false, false
		for _, s := range splits {
			if s == currIdx {
				c = true
			} else if s == currIdx+1 {
				cp1 = true
				break
			} else if s > currIdx+1 {
				break
			}
		}
		if c && cp1 { // Sub-element splitting is valid.
			_, f := math.Modf(splitFloat)
			// Convert from fraction of entire element to fraction of remainder.
			fr := (f - currProg) / (1.0 - currProg)
			return int64(splitFloat), fr, nil
		}
	}

	// For non-sub-element splitting, find the closest unprocessed split
	// point to our fraction.
	var prevDiff = math.MaxFloat64
	var bestS int64 = -1
	for _, s := range splits {
		if s >= safeStart && s <= endIdx {
			diff := math.Abs(splitFloat - float64(s))
			if diff <= prevDiff {
				prevDiff = diff
				bestS = s
			} else {
				break // Stop early if the difference starts increasing.
			}
		}
	}
	if bestS != -1 {
		return bestS, -1.0, nil
	}
	// Printing all splits is expensive. Instead, return the current start and
	// end indices, and fraction along with the range of the indices and how
	// many there are. This branch requires at least one split index, so we don't
	// need to bounds check the slice.
	return -1, -1.0, fmt.Errorf("failed to split DataSource (at index: %v, last index: %v) at fraction %.4f with requested splits (%v indices from %v to %v)",
		currIdx, endIdx, frac, len(splits), splits[0], splits[len(splits)-1])
}

func encodeElm(elm *FullValue, wc WindowEncoder, ec ElementEncoder) ([]byte, error) {
	var b bytes.Buffer
	if err := EncodeWindowedValueHeader(wc, elm.Windows, elm.Timestamp, elm.Pane, &b); err != nil {
		return nil, err
	}
	if err := ec.Encode(elm, &b); err != nil {
		return nil, err
	}
	return b.Bytes(), nil
}

type concatReStream struct {
	first, next ReStream
}

func (c *concatReStream) Open() (Stream, error) {
	firstStream, err := c.first.Open()
	if err != nil {
		return nil, err
	}
	return &concatStream{first: firstStream, nextStream: c.next}, nil
}

type concatStream struct {
	first      Stream
	nextStream ReStream
}

// Close nils the stream.
func (s *concatStream) Close() error {
	if s.first == nil {
		return nil
	}
	defer func() {
		s.first = nil
		s.nextStream = nil
	}()
	return s.first.Close()
}

func (s *concatStream) Read() (*FullValue, error) {
	if s.first == nil { // When the stream is closed.
		return nil, io.EOF
	}
	fv, err := s.first.Read()
	if err == nil {
		return fv, nil
	}
	if err == io.EOF {
		if err := s.first.Close(); err != nil {
			s.nextStream = nil
			return nil, err
		}
		if s.nextStream == nil {
			s.first = nil
			return nil, io.EOF
		}
		s.first, err = s.nextStream.Open()
		s.nextStream = nil
		if err != nil {
			return nil, err
		}
		fv, err := s.first.Read()
		return fv, err
	}
	return nil, err
}
