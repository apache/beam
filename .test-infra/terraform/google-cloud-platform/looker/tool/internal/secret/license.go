package secret

import (
	"gopkg.in/yaml.v3"
)

type User struct {
	FirstName string `yaml:"first_name"`
	LastName  string `yaml:"last_name"`
	Email     string `yaml:"email"`
	Password  string `yaml:"password"`
}

// Provision an initial startup user.
// See https://cloud.google.com/looker/docs/auto-provisioning-new-looker-instance
type Provision struct {
	LicenseKey string `yaml:"license_key"`
	HostUrl    string `yaml:"host_url"`
	User       *User  `yaml:"user"`
}

func (provision *Provision) MarshalBinary() ([]byte, error) {
	return yaml.Marshal(provision)
}

func (provision *Provision) SecureString() (string, error) {
	sec := &Provision{
		LicenseKey: "(hidden)",
		HostUrl:    provision.HostUrl,
	}
	if provision.User != nil {
		sec.User = &User{
			FirstName: provision.User.FirstName,
			LastName:  provision.User.LastName,
			Email:     provision.User.Email,
			Password:  "(hidden)",
		}
	}
	b, err := yaml.Marshal(sec)
	if err != nil {
		return "", err
	}
	return string(b), nil
}
