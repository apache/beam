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

package schema

import (
	"io"
	"math"
	"math/big"
	"strings"

	"github.com/apache/beam/sdks/v2/go/pkg/beam/core/graph/coder"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/internal/errors"
)

// URNDecimal identifies the Decimal logical type.
const URNDecimal = "beam:logical_type:decimal:v1"

// Decimal is an arbitrary precision decimal number, the
// beam:logical_type:decimal:v1 logical type. The value is Unscaled times ten
// to the power of minus Scale, so Decimal{Unscaled: 31415, Scale: 4} is
// 3.1415. A nil Unscaled is zero.
//
// The representation type is BYTES, encoded like the Java SDK's
// BigDecimalCoder: the scale as a varint, followed by the length prefixed two's
// complement big endian bytes of the unscaled value. Decimal can be a field of
// a row, but has no schema of its own and cannot be a top level element type.
type Decimal struct {
	Unscaled *big.Int
	Scale    int32
}

// ParseDecimal parses a decimal in plain notation with an optional sign, such
// as -100.123. Exponents are not supported.
func ParseDecimal(s string) (Decimal, error) {
	sign, body := "", s
	if strings.HasPrefix(body, "+") || strings.HasPrefix(body, "-") {
		sign, body = body[:1], body[1:]
	}
	digits, frac, _ := strings.Cut(body, ".")
	all := digits + frac
	if all == "" || strings.Trim(all, "0123456789") != "" {
		return Decimal{}, errors.Errorf("invalid decimal %q", s)
	}
	unscaled, _ := new(big.Int).SetString(sign+all, 10)
	return Decimal{Unscaled: unscaled, Scale: int32(len(frac))}, nil
}

// String returns the decimal in plain notation, such as -100.123.
func (d Decimal) String() string {
	unscaled := d.Unscaled
	if unscaled == nil {
		unscaled = new(big.Int)
	}
	digits := new(big.Int).Abs(unscaled).String()
	if d.Scale <= 0 {
		digits += strings.Repeat("0", int(-d.Scale))
	} else {
		scale := int(d.Scale)
		if len(digits) <= scale {
			digits = strings.Repeat("0", scale-len(digits)+1) + digits
		}
		digits = digits[:len(digits)-scale] + "." + digits[len(digits)-scale:]
	}
	if unscaled.Sign() < 0 {
		return "-" + digits
	}
	return digits
}

func encodeDecimal(d Decimal, w io.Writer) error {
	if err := coder.EncodeVarInt(int64(d.Scale), w); err != nil {
		return err
	}
	return coder.EncodeBytes(twosComplementBytes(d.Unscaled), w)
}

func decodeDecimal(r io.Reader) (Decimal, error) {
	scale, err := coder.DecodeVarInt(r)
	if err != nil {
		return Decimal{}, errors.Wrap(err, "decoding decimal scale")
	}
	if scale < math.MinInt32 || scale > math.MaxInt32 {
		return Decimal{}, errors.Errorf("decimal scale %v out of range", scale)
	}
	b, err := coder.DecodeBytes(r)
	if err != nil {
		return Decimal{}, errors.Wrap(err, "decoding decimal unscaled value")
	}
	return Decimal{Unscaled: bigIntFromTwosComplement(b), Scale: int32(scale)}, nil
}

// twosComplementBytes returns the minimal big endian two's complement bytes
// of x, as java.math.BigInteger.toByteArray does. A nil x is zero.
func twosComplementBytes(x *big.Int) []byte {
	if x == nil || x.Sign() == 0 {
		return []byte{0}
	}
	if x.Sign() > 0 {
		b := x.Bytes()
		if b[0]&0x80 != 0 {
			b = append([]byte{0}, b...)
		}
		return b
	}
	// The length is the bits of |x| - 1 plus a sign bit, rounded up to bytes.
	// The bytes hold 2^(8n) + x.
	abs := new(big.Int).Neg(x)
	n := new(big.Int).Sub(abs, big.NewInt(1)).BitLen()/8 + 1
	v := new(big.Int).Lsh(big.NewInt(1), uint(8*n))
	return v.Add(v, x).Bytes()
}

// bigIntFromTwosComplement returns the value of the big endian two's
// complement bytes b.
func bigIntFromTwosComplement(b []byte) *big.Int {
	x := new(big.Int).SetBytes(b)
	if len(b) > 0 && b[0]&0x80 != 0 {
		x.Sub(x, new(big.Int).Lsh(big.NewInt(1), uint(8*len(b))))
	}
	return x
}
