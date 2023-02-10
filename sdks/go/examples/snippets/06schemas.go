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

package snippets

import (
	"fmt"
	"io"
	"reflect"
	"time"

	"github.com/apache/beam/sdks/v2/go/pkg/beam"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/core/graph/coder"
)

// [START schema_define]

type Purchase struct {
	// ID of the user who made the purchase.
	UserID string `beam:"userId"`
	// Identifier of the item that was purchased.
	ItemID int64 `beam:"itemId"`
	// The shipping address, a nested type.
	ShippingAddress ShippingAddress `beam:"shippingAddress"`
	// The cost of the item in cents.
	Cost int64 `beam:"cost"`
	// The transactions that paid for this purchase.
	// A slice since the purchase might be spread out over multiple
	// credit cards.
	Transactions []Transaction `beam:"transactions"`
}

type ShippingAddress struct {
	StreetAddress string  `beam:"streetAddress"`
	City          string  `beam:"city"`
	State         *string `beam:"state"`
	Country       string  `beam:"country"`
	PostCode      string  `beam:"postCode"`
}

type Transaction struct {
	Bank           string  `beam:"bank"`
	PurchaseAmount float64 `beam:"purchaseAmount"`
}

// [END schema_define]

// Validate that the interface is being implemented.
var _ beam.SchemaProvider = &TimestampNanosProvider{}

// [START schema_logical_provider]

// TimestampNanos is a logical type using time.Time, but
// encodes as a schema type.
type TimestampNanos time.Time

func (tn TimestampNanos) Seconds() int64 {
	return time.Time(tn).Unix()
}
func (tn TimestampNanos) Nanos() int32 {
	return int32(time.Time(tn).UnixNano() % 1000000000)
}

// tnStorage is the storage schema for TimestampNanos.
type tnStorage struct {
	Seconds int64 `beam:"seconds"`
	Nanos   int32 `beam:"nanos"`
}

var (
	// reflect.Type of the Value type of TimestampNanos
	tnType        = reflect.TypeOf((*TimestampNanos)(nil)).Elem()
	tnStorageType = reflect.TypeOf((*tnStorage)(nil)).Elem()
)

// TimestampNanosProvider implements the beam.SchemaProvider interface.
type TimestampNanosProvider struct{}

// FromLogicalType converts checks if the given type is TimestampNanos, and if so
// returns the storage type.
func (p *TimestampNanosProvider) FromLogicalType(rt reflect.Type) (reflect.Type, error) {
	if rt != tnType {
		return nil, fmt.Errorf("unable to provide schema.LogicalType for type %v, want %v", rt, tnType)
	}
	return tnStorageType, nil
}

// BuildEncoder builds a Beam schema encoder for the TimestampNanos type.
func (p *TimestampNanosProvider) BuildEncoder(rt reflect.Type) (func(any, io.Writer) error, error) {
	if _, err := p.FromLogicalType(rt); err != nil {
		return nil, err
	}
	enc, err := coder.RowEncoderForStruct(tnStorageType)
	if err != nil {
		return nil, err
	}
	return func(iface any, w io.Writer) error {
		v := iface.(TimestampNanos)
		return enc(tnStorage{
			Seconds: v.Seconds(),
			Nanos:   v.Nanos(),
		}, w)
	}, nil
}

// BuildDecoder builds a Beam schema decoder for the TimestampNanos type.
func (p *TimestampNanosProvider) BuildDecoder(rt reflect.Type) (func(io.Reader) (any, error), error) {
	if _, err := p.FromLogicalType(rt); err != nil {
		return nil, err
	}
	dec, err := coder.RowDecoderForStruct(tnStorageType)
	if err != nil {
		return nil, err
	}
	return func(r io.Reader) (any, error) {
		s, err := dec(r)
		if err != nil {
			return nil, err
		}
		tn := s.(tnStorage)
		return TimestampNanos(time.Unix(tn.Seconds, int64(tn.Nanos))), nil
	}, nil
}

// [END schema_logical_provider]

func LogicalTypeExample() {
	// [START schema_logical_register]
	beam.RegisterSchemaProvider(tnType, &TimestampNanosProvider{})
	// [END schema_logical_register]
}
