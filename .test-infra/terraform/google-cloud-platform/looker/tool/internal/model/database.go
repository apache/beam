package model

import (
	"bytes"
	"text/template"
)

type DatabaseCredentials struct {
	Dialect  string
	Host     string
	Username string
	Password string
	Database string
	Port     string
}

func (credentials *DatabaseCredentials) MarshalBinary() ([]byte, error) {
	tmplStr := `dialect={{ .Dialect }}&host={{ .Host }}&username={{ .Username }}&database={{ .Database }}&port={{ .Port }}&password={{ .Password }}`
	tmpl := template.Must(template.New("form").Parse(tmplStr))
	buf := bytes.Buffer{}
	if err := tmpl.Execute(&buf, credentials); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}
