package secret

import "gopkg.in/yaml.v3"

type DatabaseCredentials struct {
	Dialect  string `yaml:"dialect"`
	Host     string `yaml:"host"`
	Username string `yaml:"username"`
	Password string `yaml:"password"`
	Database string `yaml:"database"`
	Port     int    `yaml:"port"`
}

func (credentials *DatabaseCredentials) MarshalBinary() ([]byte, error) {
	return yaml.Marshal(credentials)
}
