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
	"github.com/apache/beam/sdks/v2/go/pkg/beam/log"
)

type UserTimerAdapter interface {
	NewTimerProvider(ctx context.Context, manager DataManager, inputTimestamp typex.EventTime, windows []typex.Window, element *MainInput) (timerProvider, error)
}

type userTimerAdapter struct {
	sID            StreamID
	wc             WindowEncoder
	kc             ElementEncoder
	dc             ElementDecoder
	timerIDToCoder map[string]*coder.Coder
	c              *coder.Coder
}

func NewUserTimerAdapter(sID StreamID, c *coder.Coder, timerCoders map[string]*coder.Coder) UserTimerAdapter {
	if !coder.IsW(c) {
		panic(fmt.Sprintf("expected WV coder for user timer %v: %v", sID, c))
	}

	wc := MakeWindowEncoder(c.Window)
	kc := MakeElementEncoder(coder.SkipW(c).Components[0])
	dc := MakeElementDecoder(coder.SkipW(c).Components[0])

	return &userTimerAdapter{sID: sID, wc: wc, kc: kc, dc: dc, c: c, timerIDToCoder: timerCoders}
}

func (u *userTimerAdapter) NewTimerProvider(ctx context.Context, manager DataManager, inputTs typex.EventTime, w []typex.Window, element *MainInput) (timerProvider, error) {
	if u.kc == nil {
		return timerProvider{}, fmt.Errorf("cannot make a state provider for an unkeyed input %v", element)
	}
	elementKey, err := EncodeElement(u.kc, element.Key.Elm)
	if err != nil {
		return timerProvider{}, err
	}

	tp := timerProvider{
		ctx:             ctx,
		tm:              manager,
		elementKey:      elementKey,
		inputTimestamp:  inputTs,
		sID:             u.sID,
		window:          w,
		writersByFamily: make(map[string]io.Writer),
		codersByFamily:  u.timerIDToCoder,
	}

	return tp, nil
}

type timerProvider struct {
	ctx            context.Context
	tm             DataManager
	sID            StreamID
	inputTimestamp typex.EventTime
	elementKey     []byte
	window         []typex.Window

	pn typex.PaneInfo

	writersByFamily map[string]io.Writer
	codersByFamily  map[string]*coder.Coder
}

func (p *timerProvider) getWriter(family string) (io.Writer, error) {
	if w, ok := p.writersByFamily[family]; ok {
		return w, nil
	} else {
		w, err := p.tm.OpenTimerWrite(p.ctx, p.sID, family)
		if err != nil {
			return nil, err
		}
		p.writersByFamily[family] = w
		return p.writersByFamily[family], nil
	}
}

func (p *timerProvider) Set(t timers.TimerMap) {
	w, err := p.getWriter(t.Family)
	if err != nil {
		panic(err)
	}
	tm := typex.TimerMap{
		Key:           p.elementKey, //string(p.elementKey),
		Tag:           t.Tag,
		Windows:       p.window,
		Clear:         t.Clear,
		FireTimestamp: t.FireTimestamp,
		HoldTimestamp: t.HoldTimestamp,
		Pane:          p.pn,
	}
	log.Debugf(p.ctx, "timer set: %+v", tm)
	fv := FullValue{Elm: tm}
	enc := MakeElementEncoder(coder.SkipW(p.codersByFamily[t.Family]))
	if err := enc.Encode(&fv, w); err != nil {
		panic(err)
	}
}

type TimerRecv struct {
	Key                          *FullValue
	Tag                          string
	Windows                      []typex.Window // []typex.Window
	Clear                        bool
	FireTimestamp, HoldTimestamp mtime.Time
	Pane                         typex.PaneInfo
}
