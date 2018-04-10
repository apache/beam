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

import "context"

// Discard silently discard all elements. It is implicitly inserted for any
// loose ends in the pipeline.
type Discard struct {
	// UID is the unit identifier.
	UID UnitID
}

func (d *Discard) ID() UnitID {
	return d.UID
}

func (d *Discard) Up(ctx context.Context) error {
	return nil
}

func (d *Discard) StartBundle(ctx context.Context, id string, data DataManager) error {
	return nil
}

func (d *Discard) ProcessElement(ctx context.Context, value FullValue, values ...ReStream) error {
	return nil
}

func (d *Discard) FinishBundle(ctx context.Context) error {
	return nil
}

func (d *Discard) Down(ctx context.Context) error {
	return nil
}

func (d *Discard) String() string {
	return "Discard"
}
