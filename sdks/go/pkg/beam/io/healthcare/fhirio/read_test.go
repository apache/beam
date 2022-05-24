package fhirio

import (
	"errors"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/testing/passert"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/testing/ptest"
	"net/http"
	"strings"
	"testing"
)

func Test_readFailures(t *testing.T) {
	testCases := []struct {
		name           string
		client         fhirStoreClient
		containedError string
	}{
		{
			name: "Read Request Failed",
			client: &fakeFhirStoreClient{
				fakeReadResources: func(resource string) (*http.Response, error) {
					return nil, errors.New("")
				},
			},
			containedError: "Failed to fetch resource",
		},
		{
			name: "Read Request Returns Bad Status",
			client: &fakeFhirStoreClient{
				fakeReadResources: func(resource string) (*http.Response, error) {
					return &http.Response{StatusCode: 403}, nil
				},
			},
			containedError: "bad status",
		},
		{
			name: "Response body fails to be parsed",
			client: &fakeFhirStoreClient{
				fakeReadResources: func(resource string) (*http.Response, error) {
					return &http.Response{Body: &fakeReaderCloser{
						fakeRead: func([]byte) (int, error) {
							return 0, errors.New("")
						},
					}, StatusCode: 200}, nil
				},
			},
			containedError: "Error while reading response body",
		},
	}

	testResourceIds := []string{"foo", "bar"}
	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			p, s, resourceIds := ptest.CreateList(testResourceIds)
			resources, failedReads := read(s, resourceIds, testCase.client)
			passert.Empty(s, resources)
			passert.Count(s, failedReads, "", len(testResourceIds))
			passert.True(s, failedReads, func(errorMsg string) bool {
				return strings.Contains(errorMsg, testCase.containedError)
			})
			ptest.RunAndValidate(t, p)
		})
	}
}
