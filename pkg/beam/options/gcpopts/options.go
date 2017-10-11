// Package gcpopts contains shared options for Google Cloud Platform.
package gcpopts

import (
	"flag"
	"log"
)

var (
	// Project is the Google Cloud Platform project ID.
	Project = flag.String("project", "", "Google Cloud Platform project ID.")
)

// GetProject returns the project, if non empty and log.Fatals otherwise.
// Convenience function.
func GetProject() string {
	if *Project == "" {
		log.Fatal("No Google Cloud project specified. Use --project=<project>")
	}
	return *Project
}
