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

	"github.com/apache/beam/sdks/go/pkg/beam/core/graph"
	"github.com/apache/beam/sdks/go/pkg/beam/core/graph/coder"
)

// DataSink is a Node.
type DataSink struct {
	UID  UnitID
	Edge *graph.MultiEdge

	w io.WriteCloser
}

func (n *DataSink) ID() UnitID {
	return n.UID
}

func (n *DataSink) Up(ctx context.Context) error {
	return nil
}

func (n *DataSink) StartBundle(ctx context.Context, id string, data DataManager) error {
	sid := StreamID{Port: *n.Edge.Port, Target: *n.Edge.Target, InstID: id}

	w, err := data.OpenWrite(ctx, sid)
	if err != nil {
		return err
	}
	n.w = w
	return nil
}

func (n *DataSink) ProcessElement(ctx context.Context, value FullValue, values ...ReStream) error {
	// Marshal the pieces into a temporary buffer since they must be transmitted on FnAPI as a single
	// unit.
	var b bytes.Buffer

	c := n.Edge.Input[0].From.Coder
	if err := EncodeWindowedValueHeader(c, value.Timestamp, &b); err != nil {
		return err
	}
	if err := EncodeElement(coder.SkipW(c), value, &b); err != nil {
		return err
	}

	if _, err := n.w.Write(b.Bytes()); err != nil {
		return err
	}
	return nil
}

func (n *DataSink) FinishBundle(ctx context.Context) error {
	return n.w.Close()
}

func (n *DataSink) Down(ctx context.Context) error {
	return nil
}

func (n *DataSink) String() string {
	sid := StreamID{Port: *n.Edge.Port, Target: *n.Edge.Target}
	return fmt.Sprintf("DataSink[%v]", sid)
}
