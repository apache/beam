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

package mongodbio

import (
	"testing"

	"github.com/google/go-cmp/cmp"
	"go.mongodb.org/mongo-driver/bson"
)

func Test_idRangeTracker_TryClaim(t *testing.T) {
	tests := []struct {
		name          string
		tracker       *idRangeTracker
		pos           any
		wantOk        bool
		wantClaimed   int64
		wantClaimedID any
		wantDone      bool
		wantErr       bool
	}{
		{
			name: "Return true when claimed count < total count - 1",
			tracker: &idRangeTracker{
				rest: idRangeRestriction{
					Count: 10,
				},
				claimed:   5,
				claimedID: 123,
			},
			pos:           cursorResult{nextID: 124},
			wantOk:        true,
			wantClaimed:   6,
			wantClaimedID: 124,
			wantDone:      false,
		},
		{
			name: "Return true and set to done when claimed count == total count - 1",
			tracker: &idRangeTracker{
				rest: idRangeRestriction{
					Count: 10,
				},
				claimed:   9,
				claimedID: 123,
			},
			pos:           cursorResult{nextID: 124},
			wantOk:        true,
			wantClaimed:   10,
			wantClaimedID: 124,
			wantDone:      true,
		},
		{
			name: "Return false and set to done when cursor is exhausted",
			tracker: &idRangeTracker{
				rest: idRangeRestriction{
					Count: 10,
				},
				claimed:   5,
				claimedID: 123,
			},
			pos:           cursorResult{nextID: 124, isExhausted: true},
			wantOk:        false,
			wantClaimed:   5,
			wantClaimedID: 123,
			wantDone:      true,
		},
		{
			name: "Return false when claimed count == total count",
			tracker: &idRangeTracker{
				rest: idRangeRestriction{
					Count: 10,
				},
				claimed:   10,
				claimedID: 123,
			},
			pos:           cursorResult{nextID: 124},
			wantOk:        false,
			wantClaimed:   10,
			wantClaimedID: 123,
			wantDone:      true,
		},
		{
			name: "Return false when tracker is stopped",
			tracker: &idRangeTracker{
				rest: idRangeRestriction{
					Count: 10,
				},
				claimed:   10,
				claimedID: 123,
				stopped:   true,
			},
			pos:           cursorResult{nextID: 124},
			wantOk:        false,
			wantClaimed:   10,
			wantClaimedID: 123,
			wantDone:      true,
		},
		{
			name: "Return false and set error when pos is of invalid type",
			tracker: &idRangeTracker{
				rest: idRangeRestriction{
					Count: 10,
				},
				claimed:   5,
				claimedID: 123,
			},
			pos:           "invalid",
			wantOk:        false,
			wantClaimed:   5,
			wantClaimedID: 123,
			wantDone:      false,
			wantErr:       true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if gotOk := tt.tracker.TryClaim(tt.pos); gotOk != tt.wantOk {
				t.Errorf("TryClaim() = %v, want %v", gotOk, tt.wantOk)
			}
			if gotClaimed := tt.tracker.claimed; gotClaimed != tt.wantClaimed {
				t.Errorf("claimed = %v, want %v", gotClaimed, tt.wantClaimed)
			}
			if gotClaimedID := tt.tracker.claimedID; !cmp.Equal(gotClaimedID, tt.wantClaimedID) {
				t.Errorf("claimedID = %v, want %v", gotClaimedID, tt.wantClaimedID)
			}
			if gotDone := tt.tracker.IsDone(); gotDone != tt.wantDone {
				t.Errorf("IsDone() = %v, want %v", gotDone, tt.wantDone)
			}
			if gotErr := tt.tracker.GetError(); (gotErr != nil) != tt.wantErr {
				t.Errorf("GetError() error = %v, wantErr %v", gotErr, tt.wantErr)
			}
		})
	}
}

func Test_idRangeTracker_TrySplit(t *testing.T) {
	tests := []struct {
		name         string
		tracker      *idRangeTracker
		fraction     float64
		wantPrimary  any
		wantResidual any
		wantErr      bool
		wantDone     bool
	}{
		{
			name: "Primary contains no more work and residual contains all remaining work when fraction is 0",
			tracker: &idRangeTracker{
				rest: idRangeRestriction{
					IDRange: idRange{
						Min:          0,
						MinInclusive: true,
						Max:          100,
						MaxInclusive: false,
					},
					CustomFilter: bson.M{"key": "val"},
					Count:        100,
				},
				claimed:   70,
				claimedID: 69,
			},
			fraction: 0,
			wantPrimary: idRangeRestriction{
				IDRange: idRange{
					Min:          0,
					MinInclusive: true,
					Max:          69,
					MaxInclusive: true,
				},
				CustomFilter: bson.M{"key": "val"},
				Count:        70,
			},
			wantResidual: idRangeRestriction{
				IDRange: idRange{
					Min:          69,
					MinInclusive: false,
					Max:          100,
					MaxInclusive: false,
				},
				CustomFilter: bson.M{"key": "val"},
				Count:        30,
			},
			wantDone: true,
		},
		{
			name: "Primary contains all original work and residual is nil when fraction is 1",
			tracker: &idRangeTracker{
				rest: idRangeRestriction{
					IDRange: idRange{
						Min:          0,
						MinInclusive: true,
						Max:          100,
						MaxInclusive: false,
					},
					CustomFilter: bson.M{"key": "val"},
					Count:        100,
				},
				claimed:   70,
				claimedID: 69,
			},
			fraction: 1,
			wantPrimary: idRangeRestriction{
				IDRange: idRange{
					Min:          0,
					MinInclusive: true,
					Max:          100,
					MaxInclusive: false,
				},
				CustomFilter: bson.M{"key": "val"},
				Count:        100,
			},
			wantResidual: nil,
			wantDone:     false,
		},
		{
			name: "Primary contains all original work and residual is nil when the total count has been claimed",
			tracker: &idRangeTracker{
				rest: idRangeRestriction{
					IDRange: idRange{
						Min:          0,
						MinInclusive: true,
						Max:          100,
						MaxInclusive: false,
					},
					CustomFilter: bson.M{"key": "val"},
					Count:        100,
				},
				claimed:   100,
				claimedID: 99,
			},
			fraction: 1,
			wantPrimary: idRangeRestriction{
				IDRange: idRange{
					Min:          0,
					MinInclusive: true,
					Max:          100,
					MaxInclusive: false,
				},
				CustomFilter: bson.M{"key": "val"},
				Count:        100,
			},
			wantResidual: nil,
			wantDone:     true,
		},
		{
			name: "Error - fraction is less than 0",
			tracker: &idRangeTracker{
				rest: idRangeRestriction{Count: 100},
			},
			fraction: -0.1,
			wantErr:  true,
		},
		{
			name: "Error - fraction is greater than 1",
			tracker: &idRangeTracker{
				rest: idRangeRestriction{Count: 100},
			},
			fraction: 1.1,
			wantErr:  true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gotPrimary, gotResidual, err := tt.tracker.TrySplit(tt.fraction)
			if (err != nil) != tt.wantErr {
				t.Fatalf("TrySplit() error = %v, wantErr %v", err, tt.wantErr)
			}
			if diff := cmp.Diff(gotPrimary, tt.wantPrimary); diff != "" {
				t.Errorf("TrySplit() gotPrimary mismatch (-want +got):\n%s", diff)
			}
			if diff := cmp.Diff(gotResidual, tt.wantResidual); diff != "" {
				t.Errorf("TrySplit() gotResidual mismatch (-want +got):\n%s", diff)
			}
			if tt.tracker.IsDone() != tt.wantDone {
				t.Errorf("IsDone() = %v, want %v", tt.tracker.IsDone(), tt.wantDone)
			}
		})
	}
}

func Test_idRangeTracker_cutRestriction(t *testing.T) {
	tests := []struct {
		name          string
		tracker       *idRangeTracker
		wantDone      idRangeRestriction
		wantRemaining idRangeRestriction
	}{
		{
			name: "The tracker's claimedID is used as the max (inclusive) in the done restriction " +
				"and as the min (exclusive) in the remaining restriction when claimedID is not nil",
			tracker: &idRangeTracker{
				rest: idRangeRestriction{
					IDRange: idRange{
						Min:          0,
						MinInclusive: true,
						Max:          100,
						MaxInclusive: false,
					},
					CustomFilter: bson.M{"key": "val"},
					Count:        100,
				},
				claimed:   70,
				claimedID: 69,
			},
			wantDone: idRangeRestriction{
				IDRange: idRange{
					Min:          0,
					MinInclusive: true,
					Max:          69,
					MaxInclusive: true,
				},
				CustomFilter: bson.M{"key": "val"},
				Count:        70,
			},
			wantRemaining: idRangeRestriction{
				IDRange: idRange{
					Min:          69,
					MinInclusive: false,
					Max:          100,
					MaxInclusive: false,
				},
				CustomFilter: bson.M{"key": "val"},
				Count:        30,
			},
		},
		{
			name: "The tracker's restriction's min ID is used as the max (exclusive) in the done restriction " +
				"and as the min in the remaining restriction when claimedID is nil",
			tracker: &idRangeTracker{
				rest: idRangeRestriction{
					IDRange: idRange{
						Min:          0,
						MinInclusive: false,
						Max:          100,
						MaxInclusive: false,
					},
					CustomFilter: bson.M{"key": "val"},
					Count:        100,
				},
				claimed:   0,
				claimedID: nil,
			},
			wantDone: idRangeRestriction{
				IDRange: idRange{
					Min:          0,
					MinInclusive: false,
					Max:          0,
					MaxInclusive: false,
				},
				CustomFilter: bson.M{"key": "val"},
				Count:        0,
			},
			wantRemaining: idRangeRestriction{
				IDRange: idRange{
					Min:          0,
					MinInclusive: false,
					Max:          100,
					MaxInclusive: false,
				},
				CustomFilter: bson.M{"key": "val"},
				Count:        100,
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gotDone, gotRemaining := tt.tracker.cutRestriction()
			if diff := cmp.Diff(gotDone, tt.wantDone); diff != "" {
				t.Errorf("cutRestriction() gotDone mismatch (-want +got):\n%s", diff)
			}
			if diff := cmp.Diff(gotRemaining, tt.wantRemaining); diff != "" {
				t.Errorf("cutRestriction() gotRemaining mismatch (-want +got):\n%s", diff)
			}
		})
	}
}

func Test_idRangeTracker_GetProgress(t *testing.T) {
	tracker := &idRangeTracker{
		rest: idRangeRestriction{
			Count: 100,
		},
		claimed: 30,
	}
	wantDone := float64(30)
	wantRemaining := float64(70)

	t.Run(
		"Done is represented by claimed count, and remaining by total count - claimed count",
		func(t *testing.T) {
			gotDone, gotRemaining := tracker.GetProgress()
			if gotDone != wantDone {
				t.Errorf("GetProgress() gotDone = %v, want %v", gotDone, wantDone)
			}
			if gotRemaining != wantRemaining {
				t.Errorf("GetProgress() gotRemaining = %v, want %v", gotRemaining, wantRemaining)
			}
		},
	)
}

func Test_idRangeTracker_IsDone(t *testing.T) {
	tests := []struct {
		name    string
		tracker *idRangeTracker
		want    bool
	}{
		{
			name: "True when the tracker's claimed count is equal to the total count",
			tracker: &idRangeTracker{
				rest: idRangeRestriction{
					Count: 100,
				},
				claimed: 100,
			},
			want: true,
		},
		{
			name: "True when the tracker is stopped",
			tracker: &idRangeTracker{
				rest: idRangeRestriction{
					Count: 100,
				},
				claimed: 95,
				stopped: true,
			},
			want: true,
		},
		{
			name: "False when the tracker is not stopped and its claimed count is less than the total count",
			tracker: &idRangeTracker{
				rest: idRangeRestriction{
					Count: 100,
				},
				claimed: 95,
			},
			want: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := tt.tracker.IsDone(); got != tt.want {
				t.Errorf("IsDone() = %v, want %v", got, tt.want)
			}
		})
	}
}
