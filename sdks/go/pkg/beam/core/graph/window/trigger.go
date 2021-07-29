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

package window

import (
	pipepb "github.com/apache/beam/sdks/go/pkg/beam/model/pipeline_v1"
)

// type Tr {
// 	tr *pipepb.Trigger
// }

func SetAfterAll() *pipepb.Trigger {
	return &pipepb.Trigger{
		Trigger: &pipepb.Trigger_AfterAll_{
			AfterAll: &pipepb.Trigger_AfterAll{},
		},
	}
}

func SetAfterAny() *pipepb.Trigger {
	return &pipepb.Trigger{
		Trigger: &pipepb.Trigger_AfterAny_{
			AfterAny: &pipepb.Trigger_AfterAny{},
		},
	}
}

func SetAlways() *pipepb.Trigger {
	return &pipepb.Trigger{
		Trigger: &pipepb.Trigger_Always_{
			Always: &pipepb.Trigger_Always{},
		},
	}
}

func SetDefault() *pipepb.Trigger {
	return &pipepb.Trigger{
		Trigger: &pipepb.Trigger_Default_{
			Default: &pipepb.Trigger_Default{},
		},
	}
}
