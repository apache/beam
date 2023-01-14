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
	"testing"
)

func TestDiscard(t *testing.T) {
	d := Discard{}
	if err := d.Up(context.Background()); err != nil {
		t.Errorf("Discard.Up() returned error, got %v", err)
	}
	if err := d.StartBundle(context.Background(), "", DataContext{}); err != nil {
		t.Errorf("Discard.StartBundle() returned error, got %v", err)
	}
	if err := d.ProcessElement(context.Background(), &FullValue{}); err != nil {
		t.Errorf("Discard.ProcessElement() returned error, got %v", err)
	}
	if err := d.FinishBundle(context.Background()); err != nil {
		t.Errorf("Discard.FinishBundle() returned error, got %v", err)
	}
	if err := d.Down(context.Background()); err != nil {
		t.Errorf("Discard.Down() returned error, got %v", err)
	}
}
