// Package gcpopts contains shared options for Google Cloud Platform.
package gcpopts

import (
	"context"
	"flag"

	"github.com/apache/beam/sdks/go/pkg/beam/log"
)

var (
	// Project is the Google Cloud Platform project ID.
	Project = flag.String("project", "", "Google Cloud Platform project ID.")
)

// GetProject returns the project, if non empty and exits otherwise.
// Convenience function.
func GetProject(ctx context.Context) string {
	if *Project == "" {
		log.Exit(ctx, "No Google Cloud project specified. Use --project=<project>")
	}
	return *Project
}
