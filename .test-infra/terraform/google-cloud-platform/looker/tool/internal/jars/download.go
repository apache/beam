package jars

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
)

type Version struct {
	Major int
	Minor int
}

func (version *Version) String() string {
	return fmt.Sprintf("%v.%v", version.Major, version.Minor)
}

type MetadataRequest struct {
	LookerJarUrl string
	LicenseKey   string
	Email        string
	Version      *Version
}

type metadataRequestMarshaller struct {
	LicenseKey string `json:"lic"`
	Email      string `json:"email"`
	Latest     string `json:"latest"`
	Specific   string `json:"specific,omitempty"`
}

func (request *MetadataRequest) MarshalJSON() ([]byte, error) {
	req := &metadataRequestMarshaller{
		LicenseKey: request.LicenseKey,
		Email:      request.Email,
		Latest:     "latest",
	}
	if request.Version != nil {
		req.Latest = "specific"
		req.Specific = fmt.Sprintf("looker-%s-latest.jar", request.Version.String())
	}
	return json.Marshal(req)
}

func (request *MetadataRequest) Do(ctx context.Context) (*MetadataResponse, error) {
	var result *MetadataResponse
	buf := bytes.Buffer{}
	if err := json.NewEncoder(&buf).Encode(request); err != nil {
		err = fmt.Errorf("error encoding %+v, error: %w", request, err)
		return nil, err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, request.LookerJarUrl, &buf)
	if err != nil {
		err = fmt.Errorf("error creating request: url: %s, payload: %s, error %w", request.LookerJarUrl, buf.String(), err)
		return nil, err
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		err = fmt.Errorf("error executing request: %+v, error %w", req, err)
		return nil, err
	}
	defer resp.Body.Close()
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		err = fmt.Errorf("error decoding response for request: %+v, error %w", req, err)
	}
	return result, nil
}

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
