<!--

Licensed to the Apache Software Foundation (ASF) under one or more
contributor license agreements.  See the NOTICE file distributed with
this work for additional information regarding copyright ownership.
The ASF licenses this file to You under the Apache License, Version 2.0
(the "License"); you may not use this file except in compliance with
the License.  You may obtain a copy of the License at

   http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.

-->

# Apache Beam URNs

This file serves as a central place to enumerate and document the various
URNs used in the Beam portability APIs.


## Core Transforms

### urn:beam:transform:pardo:v1

TODO(BEAM-3595): Change this to beam:transform:pardo:v1.

Represents Beam's parallel do operation.

Payload: A serialized ParDoPayload proto.

### beam:transform:group_by_key:v1

Represents Beam's group-by-key operation.

Payload: None

### beam:transform:window_into:v1

Payload: A windowing strategy id.

### beam:transform:flatten:v1

### beam:transform:read:v1

### beam:transform:impulse:v1

## Combining

If any of the combine operations are produced by an SDK, it is assumed that
the SDK understands the last three combine helper operations.

### beam:transform:combine_globally:v1

### beam:transform:combine_per_key:v1

### beam:transform:combine_grouped_values:v1

### beam:transform:combine_pgbkcv:v1

### beam:transform:combine_merge_accumulators:v1

### beam:transform:combine_extract_outputs:v1


## Other common transforms

### beam:transform:reshuffle:v1


## WindowFns

### beam:windowfn:global_windows:v0.1

TODO(BEAM-3595): Change this to beam:windowfn:global_windows:v1

### beam:windowfn:fixed_windows:v0.1

TODO(BEAM-3595): Change this to beam:windowfn:fixed_windows:v1

### beam:windowfn:sliding_windows:v0.1

TODO(BEAM-3595): Change this to beam:windowfn:sliding_windows:v1

### beam:windowfn:session_windows:v0.1

TODO(BEAM-3595): Change this to beam:windowfn:session_windows:v1


## Coders

###  beam:coder:bytes:v1

Components: None

###  beam:coder:varint:v1

Components: None

###  beam:coder:kv:v1

Components: The key and value coder, in that order.

###  beam:coder:iterable:v1

Encodes an iterable of elements.

Components: Coder for a single element.

## Internal coders

The following coders are typically not specified by manually by the user,
but are used at runtime and must be supported by every SDK.

###  beam:coder:length_prefix:v1

###  beam:coder:global_window:v1

###  beam:coder:interval_window:v1

###  beam:coder:windowed_value:v1


## Side input access

### beam:side_input:iterable:v1

### beam:side_input:multimap:v1

