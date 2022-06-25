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
	"time"

	"github.com/apache/beam/sdks/v2/go/pkg/beam/core/graph/coder"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/core/sdf"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/core/util/ioutilx"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/internal/errors"
)

// DataSource is a Root execution unit.
type DataSource struct {
	UID   UnitID
	SID   StreamID
	Name  string
	Coder *coder.Coder
	Out   Node
	PCol  PCollection // Handles size metrics. Value instead of pointer so it's initialized by default in tests.

	source DataManager
	state  StateReader

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
	return nil
}

// StartBundle initializes this datasource for the bundle.
func (n *DataSource) StartBundle(ctx context.Context, id string, data DataContext) error {
	n.mu.Lock()
	n.source = data.Data
	n.state = data.State
	n.start = time.Now()
	n.index = -1
	n.splitIdx = math.MaxInt64
	n.mu.Unlock()
	return n.Out.StartBundle(ctx, id, data)
}

// ByteCountReader is a passthrough reader that counts all the bytes read through it.
// It trusts the nested reader to return accurate byte information.
type byteCountReader struct {
	count  *int
	reader io.ReadCloser
}

func (r *byteCountReader) Read(p []byte) (int, error) {
	n, err := r.reader.Read(p)
	*r.count += n
	return n, err
}

func (r *byteCountReader) Close() error {
	return r.reader.Close()
}

func (r *byteCountReader) reset() int {
	c := *r.count
	*r.count = 0
	return c
}

// Process opens the data source, reads and decodes data, kicking off element processing.
func (n *DataSource) Process(ctx context.Context) error {
	dataReader, err := n.source.OpenRead(ctx, n.SID)
	if err != nil {
		return err
	}
	defer dataReader.Close()
	n.PCol.resetSize() // initialize the size distribution for this bundle.
	var byteCount int
	dataReaderCounted := byteCountReader{reader: dataReader, count: &byteCount}

	c := coder.SkipW(n.Coder)
	wc := MakeWindowDecoder(n.Coder.Window)

	var cp ElementDecoder          // Decoder for the primary element or the key in CoGBKs.
	var valueCoders []*coder.Coder // Decoders for each value stream in CoGBKs.

	switch {
	case coder.IsCoGBK(c):
		cp = MakeElementDecoder(c.Components[0])

		// TODO(https://github.com/apache/beam/issues/18032): Support multiple value streams (coder components) with
		// with CoGBK.
		valueCoders = []*coder.Coder{c.Components[1]}
	default:
		cp = MakeElementDecoder(c)
	}

	for {
		if n.incrementIndexAndCheckSplit() {
			return nil
		}
		// TODO(lostluck) 2020/02/22: Should we include window headers or just count the element sizes?
		ws, t, pn, err := DecodeWindowedValueHeader(wc, dataReader)
		if err != nil {
			if err == io.EOF {
				return nil
			}
			return errors.Wrap(err, "source failed")
		}

		// Decode key or parallel element.
		pe, err := cp.Decode(&dataReaderCounted)
		if err != nil {
			return errors.Wrap(err, "source decode failed")
		}
		pe.Timestamp = t
		pe.Windows = ws
		pe.Pane = pn

		var valReStreams []ReStream
		reStreamCloser := &multiOnceCloser{}
		defer reStreamCloser.Close()
		for _, cod := range valueCoders {
			values, closer, err := n.makeReStream(ctx, pe, cod, &dataReaderCounted)
			if err != nil {
				return err
			}
			valReStreams = append(valReStreams, values)
			reStreamCloser.children = append(reStreamCloser.children, closer)
		}

		if err := n.Out.ProcessElement(ctx, pe, valReStreams...); err != nil {
			return err
		}
		// Collect the actual size of the element, and reset the bytecounter reader.
		n.PCol.addSize(int64(dataReaderCounted.reset()))
		dataReaderCounted.reader = dataReader

		if err := reStreamCloser.Close(); err != nil {
			return fmt.Errorf("error closing ReStream after processing element: %w", err)
		}
	}
}

func (n *DataSource) makeReStream(ctx context.Context, key *FullValue, elemCoder *coder.Coder, bcr *byteCountReader) (ReStream, io.Closer, error) {
	// TODO(lostluck) 2020/02/22: Do we include the chunk size, or just the element sizes?
	size, err := coder.DecodeInt32(bcr.reader)
	if err != nil {
		return nil, nopCloser{}, errors.Wrap(err, "stream size decoding failed")
	}

	switch {
	case size >= 0:
		// Single chunk streams are fully read in and buffered in memory.
		stream, cleanupFn, err := readStreamToReStream(ctx, bcr, int64(size), elemCoder)
		return stream, closeFunc(cleanupFn), err
	case size == -1:
		decoder := MakeElementDecoder(elemCoder)
		// Multi-chunked stream.
		var chunkReStreams []ReStream
		chunkReStreamsCloser := &multiOnceCloser{}
		closeChunkReStreamsEarly := true
		defer func() {
			if !closeChunkReStreamsEarly {
				return
			}
			chunkReStreamsCloser.Close() // ignore error because makeReStream is already returning an error in this case.
		}()
		// createChunkReStreams appends to chunkStreams and
		// chunkStreamsCloser.children
		createChunkReStreams := func() error {
			for {
				chunkSize, err := coder.DecodeVarInt(bcr.reader)
				if err != nil {
					return errors.Wrap(err, "stream chunk size decoding failed")
				}
				// All done, escape out.
				switch {
				case chunkSize == 0: // End of stream.
					return nil
				case chunkSize > 0: // Non-zero chunk; read that many elements from the stream, and add a new ReStream to chunkReStreams.
					chunkStream, closer, err := readStreamToReStream(ctx, bcr, chunkSize, elemCoder)
					if err != nil {
						return err
					}
					chunkReStreams = append(chunkReStreams, chunkStream)
					chunkReStreamsCloser.children = append(chunkReStreamsCloser.children, closeFunc(closer))
				case chunkSize == -1: // State backed iterable!
					chunk, err := coder.DecodeVarInt(bcr.reader)
					if err != nil {
						return err
					}
					token, err := ioutilx.ReadN(bcr.reader, (int)(chunk))
					if err != nil {
						return err
					}
					chunkReStreams = append(chunkReStreams, &proxyReStream{
						open: func() (Stream, error) {
							r, err := n.state.OpenIterable(ctx, n.SID, token)
							if err != nil {
								return nil, err
							}
							// We can't re-use the original bcr, since we may get new iterables,
							// or multiple of them at the same time, but we can re-use the count itself.
							r = &byteCountReader{reader: r, count: bcr.count}
							return &elementStream{r: r, ec: decoder}, nil
						},
					})
					return nil
				default:
					return errors.Errorf("multi-chunk stream with invalid chunk size of %d", chunkSize)
				}
			}
		}
		if err := createChunkReStreams(); err != nil {
			return nil, nopCloser{}, err
		}
		closeChunkReStreamsEarly = false
		return newConcatReStream(chunkReStreams...), chunkReStreamsCloser, nil
	default:
		return nil, nopCloser{}, errors.Errorf("received stream with marker size of %d", size)
	}
}

var readStreamToReStream ReStreamFactory = DefaultReadStreamToReStream

// ReStreamFactory is a function that constructs a ReStream from an io.Reader
// and a coder for type of elements that need to be decoded. A ReStreamFactory
// is used by the SDK hardness to transform a byte stream into a stream of
// FullValues while executing a DoFn that takes an iterator as once of its
// arguments (GBK and CoGBK DoFns).
//
// The factory should return a ReStream that decodes numElements elements from
// the encodedStream reader. After the DoFn that uses the stream has finished,
// the second return value will be called to close the ReStream; this provides
// the factory an opportunity to release any resources associated with the
// returned ReStream.
//
// DefaultReadSTreamToReStream is the default ReStreamFactory that is used by
// the exec package
type ReStreamFactory func(ctx context.Context, encodedStream io.Reader, numElements int64, coder *coder.Coder) (ReStream, func() error, error)

// SetReStreamFactory overrides the default behavior for constructing a ReStream
// for DoFns that iterate over values (GBK and CoGBK).
//
// The default implementation of this function is DefaultReadStreamToBuffer.
func SetReStreamFactory(fn ReStreamFactory) {
	readStreamToReStream = fn
}

// DefaultReadStreamToReStream reads numElements from the byteStream using the
// element decoder dec and returns an in-memory ReStream.
func DefaultReadStreamToReStream(_ context.Context, encodedStream io.Reader, numElements int64, coder *coder.Coder) (ReStream, func() error, error) {
	buf, err := defaultReadStreamToBuffer(encodedStream, numElements, MakeElementDecoder(coder))
	if err != nil {
		return nil, func() error { return nil }, err
	}
	return &FixedReStream{buf}, func() error { return nil }, nil
}

func defaultReadStreamToBuffer(encodedStream io.Reader, numElements int64, dec ElementDecoder) ([]FullValue, error) {
	buf := make([]FullValue, 0, numElements)
	for i := int64(0); i < numElements; i++ {
		value, err := dec.Decode(encodedStream)
		if err != nil {
			return nil, fmt.Errorf("stream value decode failed: %w", err)
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
	return fmt.Sprintf("DataSource[%v, %v] Coder:%v Out:%v", n.SID, n.Name, n.Coder, n.Out.ID())
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

	pcol PCollectionSnapshot
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
	n.mu.Unlock()
	// Do not sent negative progress reports, index is initialized to 0.
	if c < 0 {
		c = 0
	}
	pcol.ElementCount = c
	return ProgressReportSnapshot{ID: n.SID.PtransformID, Name: n.Name, Count: c, pcol: pcol}
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

// Checkpoint attempts to split an SDF that has self-checkpointed (e.g. returned a
// ProcessContinuation) and needs to be resumed later. If the underlying DoFn is not
// splittable or has not returned a resuming continuation, the function returns an empty
// SplitResult, a negative resumption time, and a false boolean to indicate that no split
// occurred.
func (n *DataSource) Checkpoint() (SplitResult, time.Duration, bool, error) {
	n.mu.Lock()
	defer n.mu.Unlock()

	pc := n.getProcessContinuation()
	if pc == nil || !pc.ShouldResume() {
		return SplitResult{}, -1 * time.Minute, false, nil
	}

	su := SplittableUnit(n.Out.(*ProcessSizedElementsAndRestrictions))

	ow := su.GetOutputWatermark()

	// Checkpointing is functionally a split at fraction 0.0
	rs, err := su.Checkpoint()
	if err != nil {
		return SplitResult{}, -1 * time.Minute, false, err
	}
	if len(rs) == 0 {
		return SplitResult{}, -1 * time.Minute, false, nil
	}

	encodeElms := n.makeEncodeElms()

	rsEnc, err := encodeElms(rs)
	if err != nil {
		return SplitResult{}, -1 * time.Minute, false, err
	}

	res := SplitResult{
		RS:   rsEnc,
		TId:  su.GetTransformId(),
		InId: su.GetInputId(),
		OW:   ow,
	}
	return res, pc.ResumeDelay(), true, nil
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
// 0 or less indicates that its unknown, and so uses the current known size.
func (n *DataSource) Split(splits []int64, frac float64, bufSize int64) (SplitResult, error) {
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
		return SplitResult{}, err
	}

	// No fraction returned, perform channel split.
	if fr < 0 {
		n.splitIdx = s
		return SplitResult{PI: s - 1, RI: s}, nil
	}
	// Get the output watermark before splitting to avoid accidentally overestimating
	ow := su.GetOutputWatermark()
	// Otherwise, perform a sub-element split.
	ps, rs, err := su.Split(fr)
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

func newConcatReStream(streams ...ReStream) *concatReStream {
	if len(streams) == 0 {
		streams = []ReStream{&FixedReStream{}}
	}
	first := streams[0]
	rest := streams[1:]
	if len(rest) == 0 {
		return &concatReStream{first: first, next: nil}
	}
	return &concatReStream{first: first, next: newConcatReStream(rest...)}
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

type multiOnceCloser struct {
	once     sync.Once
	err      error
	children []io.Closer
}

// Close() calls Close() on all the children the first time it is called and
// returns the first non-nil error or nil. If called multiple times, returns the
// original error but does not call Close again on the children.
func (c *multiOnceCloser) Close() error {
	c.once.Do(func() {
		for _, ch := range c.children {
			if err := ch.Close(); err != nil && c.err == nil {
				c.err = err
			}
		}
	})
	return c.err
}

type nopCloser struct{}

func (nopCloser) Close() error { return nil }

type closeFunc func() error

func (f closeFunc) Close() error { return f() }
