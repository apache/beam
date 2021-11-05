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

package runtime

import "testing"

func TestOptions(t *testing.T) {
	opt := &Options{opt: make(map[string]string)}

	if len(opt.Export().Options) != 0 {
		t.Errorf("fresh map not empty")
	}

	opt.Set("foo", "1")
	opt.Set("foo2", "2")
	opt.Set("/", "3")
	opt.Set("?", "4")

	if v := opt.Get("foo2"); v != "2" {
		t.Errorf("Get(foo2) = %v, want 2", v)
	}

	m := opt.Export()
	if len(m.Options) != 4 {
		t.Errorf("len(%v) = %v, want 4", m, len(m.Options))
	}

	opt.Set("bad", "5")

	opt.Import(m)

	m2 := opt.Export()
	if len(m2.Options) != 4 {
		t.Errorf("len(%v) = %v, want 4", m, len(m.Options))
	}
}
