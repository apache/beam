package healthcare

import (
	"context"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/core"
	healthcareapi "google.golang.org/api/healthcare/v1"
	"google.golang.org/api/option"
)

const userAgent = "apache-beam-io-google-cloud-platform-healthcare/" + core.SdkVersion

func NewGcpHealthcareService() *healthcareapi.Service {
	healthcareService, err := healthcareapi.NewService(context.Background(), option.WithUserAgent(userAgent))
	if err != nil {
		panic("Failed to initialize Google Cloud Healthcare Client. Reason: " + err.Error())
	}

	return healthcareService
}
