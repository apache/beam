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

package xlangx

import (
	"strings"
	"testing"

	"github.com/apache/beam/sdks/v2/go/pkg/beam/core"
)

func TestStartAutomated(t *testing.T) {
	if strings.HasSuffix(core.SdkVersion, ".dev") {
		t.Skipf("need a released SDK version to test auto python expansion service, got: %s", core.SdkVersion)
	}
	sp, addr, err := startPythonExpansionService("apache_beam.runners.portability.expansion_service_main", "")
	if err != nil {
		t.Fatal(err)
	}
	if addr == "" {
		t.Fatal("no address")
	}
	if err := sp(); err != nil {
		t.Fatal("error stoping service")
	}
}
