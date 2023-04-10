package model

import (
	"gopkg.in/yaml.v3"
)

type License struct {
	Key   string `yaml:"license_key"`
	Email string `yaml:"email"`
}

func (license *License) MarshalBinary() ([]byte, error) {
	return yaml.Marshal(license)
}
