package offsetrange

import (
	"errors"
	"math"
	"reflect"

	"github.com/apache/beam/sdks/go/pkg/beam"
)

func init() {
	beam.RegisterType(reflect.TypeOf((*Tracker)(nil)))
	beam.RegisterType(reflect.TypeOf((*Restriction)(nil)))
}

type Restriction struct {
	Start, End int64 // Half-closed interval with boundaries [start, end).
}

// Tracker tracks a restriction  that can be represented as a range of integer values,
// for example for byte offsets in a file, or indices in an array. Note that this tracker makes
// no assumptions about the positions of blocks within the range, so users must handle validation
// of block positions if needed.
type Tracker struct {
	Rest    Restriction
	Claimed int64 // Tracks the last claimed position.
	Stopped bool  // Tracks whether TryClaim has already indicated to stop processing elements for
	// any reason.
	Err error
}

// NewTracker is a constructor for an Tracker given a start and end range.
func NewTracker(rest Restriction) *Tracker {
	return &Tracker{
		Rest:    rest,
		Claimed: rest.Start - 1,
		Stopped: false,
		Err:     nil,
	}
}

// TryClaim accepts an int64 position and successfully claims it if that position is greater than
// the previously claimed position and less than the end of the restriction. Note that the
// Tracker is not considered done until a position >= tracker.end tries to be claimed,
// at which point this method signals to end processing.
func (tracker *Tracker) TryClaim(rawPos interface{}) bool {
	if tracker.Stopped == true {
		tracker.Err = errors.New("cannot claim work after restriction tracker returns false")
		return false
	}

	pos := rawPos.(int64)

	if pos < tracker.Rest.Start {
		tracker.Stopped = true
		tracker.Err = errors.New("position claimed is out of bounds of the restriction")
		return false
	}
	if pos <= tracker.Claimed {
		tracker.Stopped = true
		tracker.Err = errors.New("cannot claim a position lower than the previously claimed position")
		return false
	}

	tracker.Claimed = pos
	if pos >= tracker.Rest.End {
		tracker.Stopped = true
		return false
	}
	return true
}

// IsDone returns true if the most recent claimed element is past the end of the restriction.
func (tracker *Tracker) GetError() error {
	return tracker.Err
}

// TrySplit splits at the nearest integer greater than the given fraction of the remainder.
func (tracker *Tracker) TrySplit(fraction float64) (interface{}, error) {
	if tracker.Stopped || tracker.IsDone() {
		return nil, nil
	}
	if fraction < 0 || fraction > 1 {
		return nil, errors.New("fraction must be in range [0, 1]")
	}

	splitPt := tracker.Rest.Start + int64(fraction*float64(tracker.Rest.End-tracker.Rest.Start))
	if splitPt == tracker.Rest.End {
		return nil, nil
	}
	residual := Restriction{splitPt, tracker.Rest.End}
	tracker.Rest.End = splitPt
	return residual, nil
}

// GetProgress reports progress as the claimed fraction of the restriction.
func (tracker *Tracker) GetProgress() float64 {
	fraction := float64(tracker.Claimed-tracker.Rest.Start) /
		float64(tracker.Rest.End-tracker.Rest.Start)

	return math.Min(fraction, 1.0)
}

// IsDone returns true if the most recent claimed element is past the end of the restriction.
func (tracker *Tracker) IsDone() bool {
	return tracker.Claimed >= tracker.Rest.End
}
