package typex

import "time"

// This file defines data types that programs use to indicate a
// data value is representing a particular Beam concept.

// T is the universal type.
type T interface{}

// Timestamp is a time.Time that Beam understands.
type Timestamp time.Time

// EncodedData is a representation of user data that is encoded in
// compliance with Beam canonical encoding rules.
type EncodedData []byte
