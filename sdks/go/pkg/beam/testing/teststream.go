package testing

import (
	"fmt"
	"io"
	"time"

	"github.com/apache/beam/sdks/go/pkg/beam/core/graph/coder"
	"github.com/apache/beam/sdks/go/pkg/beam/core/runtime/exec"

	"github.com/apache/beam/sdks/go/pkg/beam"
	"github.com/apache/beam/sdks/go/pkg/beam/core/graph/mtime"
)

type EventType int

const (
	ELEMENT         EventType = iota
	WATERMARK       EventType = iota
	PROCESSING_TIME EventType = iota
)

type testStream struct {
	coder  beam.Coder
	events []Event
}

func (t testStream) getValueCoder() beam.Coder {
	return t.coder
}

func (t testStream) getEvents() []Event {
	return t.events
}

func FromRawEvents(coder beam.Coder, events []Event) testStream {
	return testStream{coder: coder, events: events}
}

func (t testStream) something(c beam.PCollection) beam.PCollection {
	return beam.PCollection{}
}

func Create(coder beam.Coder) *builder {
	return &builder{coder: coder}
}

type builder struct {
	coder            beam.Coder
	events           []Event
	currentWatermark time.Time
}

func (b builder) addElements(element interface{}, elements ...interface{}) error {
	firstElement := beam.TimestampedOf(element, b.currentWatermark)
	var remainingElements = make([]beam.TimestampedValue, len(elements))
	for i, e := range elements {
		remainingElements[i] = beam.TimestampedOf(e, b.currentWatermark)
	}
	return b.addElementsTimestamped(firstElement, remainingElements...)
}

func (b builder) addElementsTimestamped(element beam.TimestampedValue, elements ...beam.TimestampedValue) error {
	max_timestamp := maxTimestamp()
	if err := checkArgument(element.Timestamp.Before(max_timestamp), "Elements must have timestamps before %d", max_timestamp.Unix()); err != nil {
		return err
	}
	for _, e := range elements {
		if err := checkArgument(e.Timestamp.Before(max_timestamp), "Elements must have timestamps before %d", max_timestamp.Unix()); err != nil {
			return err
		}
	}
	b.events = append(b.events, addElementEvents(element, elements...))
	return nil
}

func (b builder) advanceWatermarkTo(newWatermark time.Time) error {
	max_timestamp := maxTimestamp()
	if err := checkArgument(!newWatermark.Before(b.currentWatermark), "The watermark must monotonically advance. Current: %d", b.currentWatermark.Unix()); err != nil {
		return err
	}
	if err := checkArgument(newWatermark.Before(max_timestamp), "The Watermark cannot progress beyond the maximum. Maximum: %d", max_timestamp.Unix()); err != nil {
		return err
	}
	b.events = append(b.events, advanceTo(newWatermark))
	b.currentWatermark = newWatermark
	return nil
}

func (b builder) advanceProcessingTime(duration time.Duration) error {
	if err := checkArgument(duration.Milliseconds() > 0, "Must advance the processing time by a positive amount of miliseconds. Got: %d", duration.Milliseconds()); err != nil {
		return err
	}
	b.events = append(b.events, advanceBy(duration))
	return nil
}

func (b builder) advanceWatermarkToInfinity() testStream {
	b.events = append(b.events, advanceTo(maxTimestamp()))
	return testStream{coder: b.coder, events: b.events}
}

type Event interface {
	getType() EventType
}

type ElementEvent struct {
	elements []beam.TimestampedValue
	eType    EventType
}

func addElementEvents(element beam.TimestampedValue, elements ...beam.TimestampedValue) ElementEvent {
	ee := ElementEvent{eType: ELEMENT}
	ee.elements = append([]beam.TimestampedValue{element}, elements...)
	return ee
}

func (ee ElementEvent) getType() EventType {
	return ee.eType
}

func (ee ElementEvent) getElements() []beam.TimestampedValue {
	return ee.elements
}

type WatermarkEvent struct {
	watermark time.Time
	eType     EventType
}

func (we WatermarkEvent) getType() EventType {
	return we.eType
}

func (we WatermarkEvent) getWatermark() time.Time {
	return we.watermark
}

func advanceTo(newWatermark time.Time) WatermarkEvent {
	return WatermarkEvent{eType: WATERMARK, watermark: newWatermark}
}

type ProcessingTimeEvent struct {
	amount time.Duration
	eType  EventType
}

func (pe ProcessingTimeEvent) getType() EventType {
	return pe.eType
}

func (pe ProcessingTimeEvent) getAmount() time.Duration {
	return pe.amount
}

func advanceBy(amount time.Duration) ProcessingTimeEvent {
	return ProcessingTimeEvent{eType: PROCESSING_TIME, amount: amount}
}

func checkArgument(condition bool, message string, v int64) error {
	if !condition {
		return fmt.Errorf(message, v)
	}
	return nil
}

func maxTimestamp() time.Time {
	return time.Unix(int64(mtime.MaxTimestamp), 0)
}

type testStreamCoder struct {
	elementCoder beam.TimestampedValueCoder
}

func NewTestStreamCoder(elementCoder beam.TimestampedValueCoder) testStreamCoder {
	return testStreamCoder{elementCoder: elementCoder}
}

func (tsc testStreamCoder) encode(value testStream, w io.Writer) error {
	events := value.getEvents()
	varIntCoder := exec.MakeElementEncoder(coder.NewVarInt())
	if err := varIntCoder.Encode(&exec.FullValue{Elm: len(events)}, w); err != nil {
		return err
	}
	for _, e := range events {
		if e.getType() == ELEMENT {
			if _, err := w.Write(intToByteArray(int(e.getType()))); err != nil {
				return err
			}
			elements := e.(ElementEvent).getElements()
			if err := varIntCoder.Encode(&exec.FullValue{Elm: len(elements)}, w); err != nil {
				return err
			}
			for _, el := range elements {
				if err := tsc.elementCoder.Encode(el, w); err != nil {
					return err
				}
			}
		} else if e.getType() == WATERMARK {
			if _, err := w.Write(intToByteArray(int(e.getType()))); err != nil {
				return err
			}
			watermark := e.(WatermarkEvent).getWatermark()
			if err := varIntCoder.Encode(&exec.FullValue{Elm: watermark.Unix()}, w); err != nil {
				return nil
			}
		} else if e.getType() == PROCESSING_TIME {
			if _, err := w.Write(intToByteArray(int(e.getType()))); err != nil {
				return err
			}
			processing := e.(ProcessingTimeEvent).getAmount()
			if err := varIntCoder.Encode(&exec.FullValue{Elm: int64(processing)}, w); err != nil {
				return nil
			}
		} else {
			return fmt.Errorf("Invalid element type: %d", e.getType())
		}
	}
	return nil
}

func (tsc testStreamCoder) decode(r io.Reader) (testStream, error) {
	errRet := fmt.Errorf("Invalid read data")
	varIntDecoder := exec.MakeElementDecoder(coder.NewVarInt())
	var f *exec.FullValue
	f, err := varIntDecoder.Decode(r)
	if err != nil {
		return testStream{}, errRet
	}
	nEvents, ok := f.Elm.(int)
	if !ok {
		return testStream{}, errRet
	}
	events := make([]Event, nEvents)
	for i := 0; i < nEvents; i++ {
		buf := make([]byte, 1)
		_, err := io.ReadFull(r, buf)
		if err != nil {
			return testStream{}, errRet
		}
		eventType := EventType(int(buf[0])) //check if valid
		switch eventType {
		case ELEMENT:
			var n *exec.FullValue
			n, err := varIntDecoder.Decode(r)
			if err != nil {
				return testStream{}, errRet
			}
			nElems, ok := n.Elm.(int)
			if !ok {
				return testStream{}, errRet
			}
			tvc := beam.TimestampedValueCoder{}
			elements := make([]beam.TimestampedValue, nElems)
			for j := 0; j < nElems; j++ {
				tv, err := tvc.Decode(r)
				if err != nil {
					return testStream{}, errRet
				}
				elements[j] = tv
			}
			events[i] = ElementEvent{elements: elements, eType: ELEMENT}
		case WATERMARK:
			var f *exec.FullValue
			f, err := varIntDecoder.Decode(r)
			if err != nil {
				return testStream{}, errRet
			}
			ins, ok := f.Elm.(int64)
			if !ok {
				return testStream{}, errRet
			}
			watermark := time.Unix(ins, 0)
			events[i] = WatermarkEvent{watermark: watermark, eType: WATERMARK}
		case PROCESSING_TIME:
			var f *exec.FullValue
			f, err := varIntDecoder.Decode(r)
			if err != nil {
				return testStream{}, errRet
			}
			ins, ok := f.Elm.(int64)
			if !ok {
				return testStream{}, errRet
			}
			events[i] = ProcessingTimeEvent{amount: time.Duration(ins), eType: PROCESSING_TIME}
		default:
			return testStream{}, fmt.Errorf("Invalid event type")
		}
	}
	return FromRawEvents(beam.Coder{}, events), nil
}

func intToByteArray(i int) []byte {
	return []byte{byte(i)}
}
