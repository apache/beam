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

package regression

import (
	"testing"

	"github.com/apache/beam/sdks/v2/go/pkg/beam/testing/ptest"
	"github.com/apache/beam/sdks/v2/go/test/integration"

	_ "github.com/apache/beam/sdks/v2/go/pkg/beam/runners/dataflow"
	_ "github.com/apache/beam/sdks/v2/go/pkg/beam/runners/flink"
	_ "github.com/apache/beam/sdks/v2/go/pkg/beam/runners/samza"
	_ "github.com/apache/beam/sdks/v2/go/pkg/beam/runners/spark"
)

func TestDirectParDo(t *testing.T) {
	integration.CheckFilters(t)
	ptest.RunAndValidate(t, DirectParDo())
}

func TestEmitParDo(t *testing.T) {
	integration.CheckFilters(t)
	ptest.RunAndValidate(t, EmitParDo())
}

func TestMultiEmitParDo(t *testing.T) {
	integration.CheckFilters(t)
	ptest.RunAndValidate(t, MultiEmitParDo())
}

func TestMixedOutputParDo(t *testing.T) {
	integration.CheckFilters(t)
	ptest.RunAndValidate(t, MixedOutputParDo())
}

func TestDirectParDoAfterGBK(t *testing.T) {
	integration.CheckFilters(t)
	ptest.RunAndValidate(t, DirectParDoAfterGBK())
}

func TestEmitParDoAfterGBK(t *testing.T) {
	integration.CheckFilters(t)
	ptest.RunAndValidate(t, EmitParDoAfterGBK())
}
