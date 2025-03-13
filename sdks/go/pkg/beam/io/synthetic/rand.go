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

package synthetic

// randWrapper is an interface that matches the subset of math/rand.Rand that
// is used elsewhere in this package. The reason this interface is used instead
// of rand.Rand directly is so that tests can replace the random number
// generator with a fake.
type randWrapper interface {
	Float64() float64
	Read([]byte) (int, error)
}
