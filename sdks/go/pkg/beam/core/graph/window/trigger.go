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
