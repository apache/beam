package model

import "os/exec"

var (
	gcmOpenSslCommand = exec.Command("openssl", "rand", "-base64", "32")
)

type CredentialsString string

func (key CredentialsString) MarshalBinary() ([]byte, error) {
	return []byte(key), nil
}

func NewGcmKey() (CredentialsString, error) {
	b, err := gcmOpenSslCommand.Output()
	if err != nil {
		return "", err
	}
	return CredentialsString(b), nil
}
