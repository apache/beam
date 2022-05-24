package fhirio

import (
	"net/http"
)

type fakeFhirStoreClient struct {
	fakeReadResources func(string) (*http.Response, error)
}

func (c *fakeFhirStoreClient) readResource(resourceName string) (*http.Response, error) {
	return c.fakeReadResources(resourceName)
}

type fakeReaderCloser struct {
	fakeRead func([]byte) (int, error)
}

func (*fakeReaderCloser) Close() error {
	return nil
}

func (m *fakeReaderCloser) Read(b []byte) (int, error) {
	return m.fakeRead(b)
}
