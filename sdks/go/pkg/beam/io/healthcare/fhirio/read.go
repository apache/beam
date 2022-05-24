package fhirio

import (
	"github.com/apache/beam/sdks/v2/go/pkg/beam"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/internal/errors"
	"io"
	"reflect"
)

func init() {
	beam.RegisterType(reflect.TypeOf((*readResourceFn)(nil)))
}

type readResourceFn struct {
	client fhirStoreClient
}

func (fn *readResourceFn) ProcessElement(resourceId string, emitResource func(string), emitDeadLetter func(string)) {
	response, err := fn.client.readResource(resourceId)
	if err != nil {
		emitDeadLetter(errors.Wrapf(err, "Failed to fetch resource [%s].", resourceId).Error())
		return
	}

	if response.StatusCode != 200 {
		emitDeadLetter(errors.Errorf("Fetched resource [%s] returned bad status [%d].", resourceId, response.StatusCode).Error())
		return
	}

	bytes, err := io.ReadAll(response.Body)
	if err != nil {
		emitDeadLetter(errors.Wrapf(err, "Error while reading response body of resource [%s]", resourceId).Error())
		return
	}

	emitResource(string(bytes))
}

func Read(s beam.Scope, resourceIds beam.PCollection) (beam.PCollection, beam.PCollection) {
	s = s.Scope("fhirio.Read")
	return read(s, resourceIds, newFhirStoreClient())
}

func read(s beam.Scope, resourceIds beam.PCollection, client fhirStoreClient) (beam.PCollection, beam.PCollection) {
	return beam.ParDo2(s, &readResourceFn{client}, resourceIds)
}
