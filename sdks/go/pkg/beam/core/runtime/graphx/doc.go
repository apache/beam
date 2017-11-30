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

// Package graphx provides facilities to help with the serialization of
// pipelines into a serializable graph structure suitable for the worker.
//
// The registry's Register function is used by transform authors to make their
// type's methods available for remote invocation.  The runner then uses the
// registry's Key and Lookup methods to access information supplied by transform
// authors.
//
// The Encode* and Decode* methods are used to handle serialization of both
// regular Go data and the specific Beam data types. The Encode* methods are
// used after pipeline construction to turn the plan into a serializable form
// that can be sent for remote execution. The Decode* methods are used by the
// runner to recover the execution plan from the serialized form.
package graphx
