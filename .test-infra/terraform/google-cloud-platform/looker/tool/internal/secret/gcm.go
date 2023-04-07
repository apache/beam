package secret

import "os/exec"

var (
	gcmOpenSslCommand = exec.Command("openssl", "rand", "-base64", "32")
)

type GcmKey string

func (key GcmKey) MarshalBinary() ([]byte, error) {
	return []byte(key), nil
}

func NewGcmKey() (GcmKey, error) {
	b, err := gcmOpenSslCommand.Output()
	if err != nil {
		return "", err
	}
	return GcmKey(b), nil
}
