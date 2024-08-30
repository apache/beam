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

package natsio

import "errors"

var (
	errInvalidFetchSize  = errors.New("fetch size must be greater than 0")
	errInvalidStartSeqNo = errors.New("start sequence number must be greater than 0")
	errInvalidEndSeqNo   = errors.New("end sequence number must be greater than 0")
)

type readOption struct {
	CredsFile  string
	TimePolicy timePolicy
	FetchSize  int
	StartSeqNo int64
	EndSeqNo   int64
}

// ReadOptionFn is a function that can be passed to Read to configure options for reading
// from NATS.
type ReadOptionFn func(option *readOption) error

// ReadUserCredentials sets the user credentials when connecting to NATS.
func ReadUserCredentials(credsFile string) ReadOptionFn {
	return func(o *readOption) error {
		o.CredsFile = credsFile
		return nil
	}
}

// ReadProcessingTimePolicy specifies that the pipeline processing time of the messages should be
// used to compute the watermark estimate.
func ReadProcessingTimePolicy() ReadOptionFn {
	return func(o *readOption) error {
		o.TimePolicy = processingTimePolicy
		return nil
	}
}

// ReadPublishingTimePolicy specifies that the publishing time of the messages should be used to
// compute the watermark estimate.
func ReadPublishingTimePolicy() ReadOptionFn {
	return func(o *readOption) error {
		o.TimePolicy = publishingTimePolicy
		return nil
	}
}

// ReadFetchSize sets the maximum number of messages to retrieve at a time.
func ReadFetchSize(size int) ReadOptionFn {
	return func(o *readOption) error {
		if size <= 0 {
			return errInvalidFetchSize
		}

		o.FetchSize = size
		return nil
	}
}

// ReadStartSeqNo sets the start sequence number of messages to read.
func ReadStartSeqNo(seqNo int64) ReadOptionFn {
	return func(o *readOption) error {
		if seqNo <= 0 {
			return errInvalidStartSeqNo
		}

		o.StartSeqNo = seqNo
		return nil
	}
}

// ReadEndSeqNo sets the end sequence number of messages to read (exclusive).
func ReadEndSeqNo(seqNo int64) ReadOptionFn {
	return func(o *readOption) error {
		if seqNo <= 0 {
			return errInvalidEndSeqNo
		}

		o.EndSeqNo = seqNo
		return nil
	}
}
