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

package cache

import (
	"context"
	"time"
)

// Event represents an event payload.
type Event []byte

// Decrementer decrements a cached quota.
type Decrementer interface {

	// Decrement a quota identified by quotaID.
	Decrement(ctx context.Context, quotaID string) error
}

// Refresher refreshes a cached quota.
type Refresher interface {

	// Refresh a cached quota with size identified by a quotaID every interval.
	Refresh(ctx context.Context, quotaID string, size uint64, interval time.Duration) error
}

// Publisher publishes an Event.
type Publisher interface {

	// Publish an Event of key.
	Publish(ctx context.Context, key string, event Event) error
}

// Subscriber subscribes to events.
type Subscriber interface {

	// Subscribe to events identified by keys.
	Subscribe(ctx context.Context, events chan Event, keys ...string) error
}
