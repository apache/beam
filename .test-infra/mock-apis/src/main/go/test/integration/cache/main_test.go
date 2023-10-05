package cache

import (
	"context"
	"testing"

	"github.com/apache/beam/test-infra/mock-apis/src/main/go/internal/logging"
	"github.com/apache/beam/test-infra/mock-apis/src/main/go/test/integration/test_framework"
)

var (
	logger = logging.New("github.com/apache/beam/test-infra/mock-apis/src/main/go/test/integration/cache")
)

func TestMain(m *testing.M) {
	ctx := context.Background()
	if err := test_framework.Run(ctx, m); err != nil {
		logger.Error(ctx, err, test_framework.LoggerFields...)
	}
}
