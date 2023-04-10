package jars

import (
	"context"
	"fmt"
	"io"
	"net/http"
)

type MetadataResponse struct {
	Url             string `json:"url"`
	Sha256          string `json:"sha256"`
	VersionText     string `json:"version_text"`
	DepSha256       string `json:"depSha256"`
	DepUrl          string `json:"depUrl"`
	DepDisplayFile  string `json:"depDisplayFile"`
	SplitJarMessage string `json:"splitJarMessage"`
}

func (response *MetadataResponse) LookerJar(ctx context.Context, w io.Writer) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, response.Url, nil)
	if err != nil {
		return fmt.Errorf("error creating request: %+v, error %w", req, err)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return fmt.Errorf("error executing request: %+v, error %w", req, err)
	}
	return resp.Write(w)
}

func (response *MetadataResponse) LookerDependencyJar(ctx context.Context, w io.Writer) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, response.DepUrl, nil)
	if err != nil {
		return fmt.Errorf("error creating request: %+v, error %w", req, err)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return fmt.Errorf("error executing request: %+v, error %w", req, err)
	}
	return resp.Write(w)
}
