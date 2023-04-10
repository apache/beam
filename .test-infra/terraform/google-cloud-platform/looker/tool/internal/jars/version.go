package jars

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/apache/beam/testinfra/terraform/googlecloudplatform/looker/cmd/common"
)

type Version struct {
	Major int
	Minor int
}

func (version *Version) String() string {
	return fmt.Sprintf("%v.%v", version.Major, version.Minor)
}

func ParseVersion(majorMinor string) (*Version, error) {
	split := strings.Split(common.LookerVersion.Value(), ".")
	if len(split) != 2 {
		return nil, fmt.Errorf("error parsing version from environment variable %s, expected <Major>.<Minor>", common.LookerVersion.KeyValue())
	}
	major, err := strconv.Atoi(split[0])
	if err != nil {
		return nil, fmt.Errorf("error parsing version from environment variable %s, expected <Major>.<Minor>", common.LookerVersion.KeyValue())
	}
	minor, err := strconv.Atoi(split[1])
	if err != nil {
		return nil, fmt.Errorf("error parsing version from environment variable %s, expected <Major>.<Minor>", common.LookerVersion.KeyValue())
	}
	return &Version{
		Major: major,
		Minor: minor,
	}, nil
}
