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
	"sync/atomic"
	"time"

	"github.com/apache/beam/sdks/go/pkg/beam/core/graph/coder"
	"github.com/apache/beam/sdks/go/pkg/beam/internal/errors"
	"github.com/apache/beam/sdks/go/pkg/beam/log"
)

// DataSink is a Node.
type DataSink struct {
	UID   UnitID
	SID   StreamID
	Coder *coder.Coder

	enc   ElementEncoder
	wEnc  WindowEncoder
	w     io.WriteCloser
	count int64
	start time.Time
}

func (n *DataSink) ID() UnitID {
	return n.UID
}

func (n *DataSink) Up(ctx context.Context) error {
	n.enc = MakeElementEncoder(coder.SkipW(n.Coder))
	n.wEnc = MakeWindowEncoder(n.Coder.Window)
	return nil
}

func (n *DataSink) StartBundle(ctx context.Context, id string, data DataContext) error {
	w, err := data.Data.OpenWrite(ctx, n.SID)
	if err != nil {
		return err
	}
	n.w = w
	atomic.StoreInt64(&n.count, 0)
	n.start = time.Now()
	return nil
}

func (n *DataSink) ProcessElement(ctx context.Context, value *FullValue, values ...ReStream) error {
	// Marshal the pieces into a temporary buffer since they must be transmitted on FnAPI as a single
	// unit.
	var b bytes.Buffer

	atomic.AddInt64(&n.count, 1)
	if err := EncodeWindowedValueHeader(n.wEnc, value.Windows, value.Timestamp, &b); err != nil {
		return err
	}
	if err := n.enc.Encode(value, &b); err != nil {
		return errors.WithContextf(err, "encoding element %v with coder %v", value, n.enc)
	}
	if _, err := n.w.Write(b.Bytes()); err != nil {
		return err
	}
	return nil
}

func (n *DataSink) FinishBundle(ctx context.Context) error {
	log.Infof(ctx, "DataSink: %d elements in %d ns", atomic.LoadInt64(&n.count), time.Now().Sub(n.start))
	return n.w.Close()
}

func (n *DataSink) Down(ctx context.Context) error {
	return nil
}

func (n *DataSink) String() string {
	return fmt.Sprintf("DataSink[%v] Coder:%v", n.SID, n.Coder)
}
