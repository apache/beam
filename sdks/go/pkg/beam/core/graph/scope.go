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

package graph

import "context"

// Scope is a syntactic Scope, such as arising from a composite Transform. It
// has no semantic meaning at execution time. Used by monitoring.
type Scope struct {
	id int

	// Label is the human-visible label for this scope.
	Label string
	// Parent is the parent scope, if nested.
	Parent *Scope
	// Context contains optional metadata associated with this scope.
	Context context.Context
}

// ID returns the graph-local identifier for the scope.
func (s *Scope) ID() int {
	return s.id
}

func (s *Scope) String() string {
	if s.Parent == nil {
		return s.Label
	}
	return s.Parent.String() + "/" + s.Label
}
