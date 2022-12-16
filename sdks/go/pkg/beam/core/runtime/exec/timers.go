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
	"github.com/apache/beam/sdks/v2/go/pkg/beam/core/timers"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/core/typex"
)

// UserTimerAdapter provides a timer provider to manipulates timers.
type UserTimerAdapter interface {
	NewTimerProvider(ctx context.Context, manager TimerManager, w []typex.Window, element any) (timerProvider, error)
}

type userTimerAdapter struct {
	sID            StreamID
	wc             WindowEncoder
	kc             ElementEncoder
	timerIDToCoder map[string]*coder.Coder
	c              *coder.Coder
}

// NewUserTimerAdapter builds and returns a new user timer adapter for the given StreamID and coder.
func NewUserTimerAdapter(sID StreamID, c *coder.Coder, timerCoders map[string]*coder.Coder) UserTimerAdapter {
	if !coder.IsW(c) {
		panic(fmt.Sprintf("expected WV coder for timer %v: %v", sID, c))
	}

	wc := MakeWindowEncoder(c.Window)
	var kc ElementEncoder
	if coder.IsKV(coder.SkipW(c)) {
		kc = MakeElementEncoder(coder.SkipW(c).Components[0])
	}
	return &userTimerAdapter{sID: sID, wc: wc, kc: kc, c: c, timerIDToCoder: timerCoders}
}

// NewTimerProvider builds and returns a new timer provider used to manipulate timers.
func (u *userTimerAdapter) NewTimerProvider(ctx context.Context, manager TimerManager, w []typex.Window, element interface{}) (timerProvider, error) {
	if u.kc == nil {
		return timerProvider{}, fmt.Errorf("cannot make a timer provider for an unkeyed input %v", element)
	}
	elementKey, err := EncodeElement(u.kc, element.(*MainInput).Key.Elm)
	if err != nil {
		return timerProvider{}, err
	}
	tp := timerProvider{
		ctx:          ctx,
		tm:           manager,
		elementKey:   elementKey,
		SID:          u.sID,
		window:       w,
		writersByKey: make(map[string]io.Writer),
		codersByKey:  u.timerIDToCoder,
	}

	return tp, nil
}

type timerProvider struct {
	ctx          context.Context
	tm           TimerManager
	SID          StreamID
	elementKey   []byte
	window       []typex.Window
	pn           typex.PaneInfo
	writersByKey map[string]io.Writer
	codersByKey  map[string]*coder.Coder
}

func (p *timerProvider) getWriter(key string) (io.Writer, error) {
	if w, ok := p.writersByKey[key]; ok {
		return w, nil
	} else {
		w, err := p.tm.OpenTimerWrite(p.ctx, p.SID, key)
		if err != nil {
			return nil, err
		}
		p.writersByKey[key] = w
		return p.writersByKey[key], nil
	}
}

// Set manipulates the timer based on the configuration(set/clear) provided
// in the input TimerMap.
func (p *timerProvider) Set(t timers.TimerMap) {
	w, err := p.getWriter(t.Key)
	if err != nil {
		panic(err)
	}
	tm := typex.TimerMap{
		Key:           t.Key,
		Tag:           t.Tag,
		Windows:       p.window,
		Clear:         t.Clear,
		FireTimestamp: t.FireTimestamp,
		HoldTimestamp: t.HoldTimestamp,
		Pane:          p.pn,
	}
	fv := FullValue{Elm: tm}
	enc := MakeElementEncoder(coder.SkipW(p.codersByKey[t.Key]))
	err = enc.Encode(&fv, w)
	if err != nil {
		panic(err)
	}
}
