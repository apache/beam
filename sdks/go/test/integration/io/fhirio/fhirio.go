package fhirio

import (
	"strings"

	"github.com/apache/beam/sdks/v2/go/pkg/beam"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/io/fhirio"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/testing/passert"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/testing/ptest"
)

func ReadPipeline(testResourcePaths []string) *beam.Pipeline {
	p, s, resourcePaths := ptest.CreateList(testResourcePaths)
	resources, failedReads := fhirio.Read(s, resourcePaths)
	passert.Empty(s, failedReads)
	passert.Count(s, resources, "", len(testResourcePaths))
	return p
}

func InvalidReadPipeline(fhirStorePath string) *beam.Pipeline {
	invalidResourcePath := fhirStorePath + "/fhir/Patient/invalid"
	p, s, resourcePaths := ptest.CreateList([]string{invalidResourcePath})
	resources, failedReads := fhirio.Read(s, resourcePaths)
	passert.Count(s, failedReads, "", 1)
	passert.Empty(s, resources)
	passert.True(s, failedReads, func(errorMsg string) bool {
		return strings.Contains(errorMsg, "bad status [404]")
	})
	return p
}
