package model

import "gopkg.in/yaml.v3"

const (
	mask = "(hidden)"
)

func (license *License) SecureString() (string, error) {
	sec := &License{
		Key:   mask,
		Email: license.Email,
	}
	b, err := yaml.Marshal(sec)
	if err != nil {
		return "", err
	}
	return string(b), nil
}

func (credentials *DatabaseCredentials) SecureString() (string, error) {
	credentialsCopy := &DatabaseCredentials{
		Dialect:  credentials.Dialect,
		Host:     credentials.Host,
		Username: credentials.Username,
		Password: mask,
		Database: credentials.Database,
		Port:     credentials.Port,
	}
	b, err := credentialsCopy.MarshalBinary()
	if err != nil {
		return "", err
	}
	return string(b), nil
}
