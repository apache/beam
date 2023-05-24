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
	"fmt"
	"io"

	"github.com/apache/beam/sdks/v2/go/pkg/beam/core/graph/coder"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/core/graph/mtime"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/core/timers"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/core/typex"
)

// UserTimerAdapter provides a timer provider to be used for manipulating timers.
type UserTimerAdapter interface {
	NewTimerProvider(ctx context.Context, manager DataManager, inputTimestamp typex.EventTime, windows []typex.Window, element *MainInput) (timerProvider, error)
}

type userTimerAdapter struct {
	sID StreamID
	ec  ElementEncoder
	dc  ElementDecoder
	wc  WindowDecoder
}

// NewUserTimerAdapter returns a user timer adapter for the given StreamID and timer coder.
func NewUserTimerAdapter(sID StreamID, c *coder.Coder, timerCoder *coder.Coder) UserTimerAdapter {
	if !coder.IsW(c) {
		panic(fmt.Sprintf("expected WV coder for user timer %v: %v", sID, c))
	}
	ec := MakeElementEncoder(timerCoder)
	dc := MakeElementDecoder(coder.SkipW(c).Components[0])
	wc := MakeWindowDecoder(c.Window)
	return &userTimerAdapter{sID: sID, ec: ec, wc: wc, dc: dc}
}

// NewTimerProvider creates and returns a timer provider to set/clear timers.
func (u *userTimerAdapter) NewTimerProvider(ctx context.Context, manager DataManager, inputTs typex.EventTime, w []typex.Window, element *MainInput) (timerProvider, error) {
	userKey := &FullValue{Elm: element.Key.Elm}
	tp := timerProvider{
		ctx:                 ctx,
		tm:                  manager,
		userKey:             userKey,
		inputTimestamp:      inputTs,
		sID:                 u.sID,
		window:              w,
		writersByFamily:     make(map[string]io.Writer),
		timerElementEncoder: u.ec,
		keyElementDecoder:   u.dc,
	}

	return tp, nil
}

type timerProvider struct {
	ctx            context.Context
	tm             DataManager
	sID            StreamID
	inputTimestamp typex.EventTime
	userKey        *FullValue
	window         []typex.Window

	pn typex.PaneInfo

	writersByFamily     map[string]io.Writer
	timerElementEncoder ElementEncoder
	keyElementDecoder   ElementDecoder
}

func (p *timerProvider) getWriter(family string) (io.Writer, error) {
	if w, ok := p.writersByFamily[family]; ok {
		return w, nil
	}
	w, err := p.tm.OpenTimerWrite(p.ctx, p.sID, family)
	if err != nil {
		return nil, err
	}
	p.writersByFamily[family] = w
	return p.writersByFamily[family], nil
}

// Set writes a new timer. This can be used to both Set as well as Clear the timer.
// Note: This function is intended for internal use only.
func (p *timerProvider) Set(t timers.TimerMap) {
	w, err := p.getWriter(t.Family)
	if err != nil {
		panic(err)
	}
	tm := TimerRecv{
		Key:           p.userKey,
		Tag:           t.Tag,
		Windows:       p.window,
		Clear:         t.Clear,
		FireTimestamp: t.FireTimestamp,
		HoldTimestamp: t.HoldTimestamp,
		Pane:          p.pn,
	}
	fv := FullValue{Elm: tm}
	if err := p.timerElementEncoder.Encode(&fv, w); err != nil {
		panic(err)
	}
}

// TimerRecv holds the timer metadata while encoding and decoding timers in exec unit.
type TimerRecv struct {
	Key                          *FullValue
	Tag                          string
	Windows                      []typex.Window // []typex.Window
	Clear                        bool
	FireTimestamp, HoldTimestamp mtime.Time
	Pane                         typex.PaneInfo
}
