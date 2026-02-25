package dot

import (
	"context"
	"os"
	"strings"
	"testing"

	"github.com/apache/beam/sdks/v2/go/pkg/beam"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/testing/passert"
)

func TestDotRunner_GeneratesDeterministicOutput(t *testing.T) {
	ctx := context.Background()

	// Create temporary DOT file
	tmpFile, err := os.CreateTemp("", "dot_test_*.dot")
	if err != nil {
		t.Fatalf("failed to create temp file: %v", err)
	}
	defer os.Remove(tmpFile.Name())

	// Set flag manually
	*dotFile = tmpFile.Name()

	// Build simple pipeline
	p, s := beam.NewPipelineWithRoot()

	col := beam.Create(s, "a", "b", "c")
	passert.Count(s, col, "", 3)

	// Run with dot runner
	_, err = Execute(ctx, p)
	if err != nil {
		t.Fatalf("Execute failed: %v", err)
	}

	// Read generated file
	data, err := os.ReadFile(tmpFile.Name())
	if err != nil {
		t.Fatalf("failed to read dot file: %v", err)
	}

	content := string(data)

	if !strings.HasPrefix(content, "digraph G {") {
		t.Fatalf("dot output missing header")
	}

	if !strings.Contains(content, "->") {
		t.Fatalf("dot output contains no edges")
	}
}
