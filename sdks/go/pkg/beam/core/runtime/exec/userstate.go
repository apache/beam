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
	"github.com/apache/beam/sdks/v2/go/pkg/beam/core/state"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/core/typex"
)

type stateProvider struct {
	ctx        context.Context
	sr         StateReader
	SID        StreamID
	elementKey []byte
	window     []byte

	transactionsByKey map[string][]state.Transaction
	initialValueByKey map[string]interface{}
	readersByKey      map[string]io.ReadCloser
	appendersByKey    map[string]io.Writer
	clearersByKey     map[string]io.Writer
	codersByKey       map[string]*coder.Coder
}

// ReadValueState reads a value state from the State API
func (s *stateProvider) ReadValueState(userStateID string) (interface{}, []state.Transaction, error) {
	initialValue, ok := s.initialValueByKey[userStateID]
	if !ok {
		rw, err := s.getReader(userStateID)
		if err != nil {
			return nil, nil, err
		}
		dec := MakeElementDecoder(coder.SkipW(s.codersByKey[userStateID]))
		resp, err := dec.Decode(rw)
		if err != nil && err != io.EOF {
			return nil, nil, err
		}
		if resp == nil {
			return nil, []state.Transaction{}, nil
		}
		initialValue = resp.Elm
	}

	transactions, ok := s.transactionsByKey[userStateID]
	if !ok {
		transactions = []state.Transaction{}
	}

	return initialValue, transactions, nil
}

// WriteValueState writes a value state to the State API
// For value states, this is done by clearing a bag state and writing a value to it.
func (s *stateProvider) WriteValueState(val state.Transaction) error {
	cl, err := s.getClearer(val.Key)
	if err != nil {
		return err
	}
	cl.Write([]byte{})

	ap, err := s.getAppender(val.Key)
	if err != nil {
		return err
	}
	fv := FullValue{Elm: val.Val}
	// TODO(#22736) - consider caching this a proprty of stateProvider
	enc := MakeElementEncoder(coder.SkipW(s.codersByKey[val.Key]))
	err = enc.Encode(&fv, ap)
	if err != nil {
		return err
	}

	// TODO(#22736) - optimize this a bit once all state types are added. In the case of sets/clears,
	// we can remove the transactions. We can also consider combining other transactions on read (or sooner)
	// so that we don't need to use as much memory/time replaying transactions.
	if transactions, ok := s.transactionsByKey[val.Key]; ok {
		transactions = append(transactions, val)
		s.transactionsByKey[val.Key] = transactions
	} else {
		s.transactionsByKey[val.Key] = []state.Transaction{val}
	}

	return nil
}

func (s *stateProvider) getReader(userStateID string) (io.ReadCloser, error) {
	if r, ok := s.readersByKey[userStateID]; ok {
		return r, nil
	} else {
		r, err := s.sr.OpenBagUserStateReader(s.ctx, s.SID, userStateID, s.elementKey, s.window)
		if err != nil {
			return nil, err
		}
		s.readersByKey[userStateID] = r
		return s.readersByKey[userStateID], nil
	}
}

func (s *stateProvider) getAppender(userStateID string) (io.Writer, error) {
	if w, ok := s.appendersByKey[userStateID]; ok {
		return w, nil
	} else {
		w, err := s.sr.OpenBagUserStateAppender(s.ctx, s.SID, userStateID, s.elementKey, s.window)
		if err != nil {
			return nil, err
		}
		s.appendersByKey[userStateID] = w
		return s.appendersByKey[userStateID], nil
	}
}

func (s *stateProvider) getClearer(userStateID string) (io.Writer, error) {
	if w, ok := s.clearersByKey[userStateID]; ok {
		return w, nil
	} else {
		w, err := s.sr.OpenBagUserStateClearer(s.ctx, s.SID, userStateID, s.elementKey, s.window)
		if err != nil {
			return nil, err
		}
		s.clearersByKey[userStateID] = w
		return s.clearersByKey[userStateID], nil
	}
}

type UserStateAdapter interface {
	NewStateProvider(ctx context.Context, reader StateReader, w typex.Window, element interface{}) (stateProvider, error)
}

type userStateAdapter struct {
	sid            StreamID
	wc             WindowEncoder
	kc             ElementEncoder
	stateIDToCoder map[string]*coder.Coder
	c              *coder.Coder
}

// NewUserStateAdapter returns a user state adapter for the given StreamID and coder.
// It expects a W<V> or W<KV<K,V>> coder, because the protocol requires windowing information.
func NewUserStateAdapter(sid StreamID, c *coder.Coder, stateIDToCoder map[string]*coder.Coder) UserStateAdapter {
	if !coder.IsW(c) {
		panic(fmt.Sprintf("expected WV coder for user state %v: %v", sid, c))
	}

	wc := MakeWindowEncoder(c.Window)
	var kc ElementEncoder
	if coder.IsKV(coder.SkipW(c)) {
		kc = MakeElementEncoder(coder.SkipW(c).Components[0])
	}
	return &userStateAdapter{sid: sid, wc: wc, kc: kc, c: c, stateIDToCoder: stateIDToCoder}
}

// NewStateProvider creates a stateProvider with the ability to talk to the state API.
func (s *userStateAdapter) NewStateProvider(ctx context.Context, reader StateReader, w typex.Window, element interface{}) (stateProvider, error) {
	if s.kc == nil {
		return stateProvider{}, fmt.Errorf("cannot make a state provider for an unkeyed input %v", element)
	}
	elementKey, err := EncodeElement(s.kc, element.(*MainInput).Key.Elm)
	if err != nil {
		return stateProvider{}, err
	}

	win, err := EncodeWindow(s.wc, w)
	if err != nil {
		return stateProvider{}, err
	}
	sp := stateProvider{
		ctx:               ctx,
		sr:                reader,
		SID:               s.sid,
		elementKey:        elementKey,
		window:            win,
		transactionsByKey: make(map[string][]state.Transaction),
		initialValueByKey: make(map[string]interface{}),
		readersByKey:      make(map[string]io.ReadCloser),
		appendersByKey:    make(map[string]io.Writer),
		clearersByKey:     make(map[string]io.Writer),
		codersByKey:       s.stateIDToCoder,
	}

	return sp, nil
}
