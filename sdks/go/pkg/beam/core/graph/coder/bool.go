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

package coder

import (
	"io"

	"github.com/apache/beam/sdks/go/pkg/beam/core/util/ioutilx"
	"github.com/apache/beam/sdks/go/pkg/beam/internal/errors"
)

// EncodeBool encodes a boolean according to the beam protocol.
func EncodeBool(v bool, w io.Writer) error {
	// Encoding: false = 0, true = 1
	var err error
	if v {
		_, err = ioutilx.WriteUnsafe(w, []byte{1})
	} else {
		_, err = ioutilx.WriteUnsafe(w, []byte{0})
	}
	if err != nil {
		return errors.Wrap(err, "error encoding bool")
	}
	return nil
}

// DecodeBool decodes a boolean according to the beam protocol.
func DecodeBool(r io.Reader) (bool, error) {
	// Encoding: false = 0, true = 1
	var b [1]byte
	if err := ioutilx.ReadNBufUnsafe(r, b[:]); err != nil {
		if err == io.EOF {
			return false, err
		}
		return false, errors.Wrap(err, "error decoding bool")
	}
	switch b[0] {
	case 0:
		return false, nil
	case 1:
		return true, nil
	}
	return false, errors.Errorf("error decoding bool: received invalid value %v", b[0])
}
