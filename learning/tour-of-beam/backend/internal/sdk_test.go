package internal

import "testing"

func TestParse(t *testing.T) {
	for _, s := range []struct {
		str      string
		expected Sdk
	}{
		{"Go", SDK_GO},
		{"Python", SDK_PYTHON},
		{"Java", SDK_JAVA},
		{"SCIO", SDK_SCIO},
		{"Bad", SDK_UNDEFINED},
		{"", SDK_UNDEFINED},
	} {
		if parsed := FromString(s.str); parsed != s.expected {
			t.Errorf("Failed to parse %v: got %v (expected %v)", s.str, parsed, s.expected)
		}
	}
}

func TestSerialize(t *testing.T) {
	for _, s := range []struct {
		expected string
		sdk      Sdk
	}{
		{"Go", SDK_GO},
		{"Python", SDK_PYTHON},
		{"Java", SDK_JAVA},
		{"SCIO", SDK_SCIO},
		{"", SDK_UNDEFINED},
	} {
		if txt := s.sdk.String(); txt != s.expected {
			t.Errorf("Failed to serialize %v to string: got %v (expected %v)", s.sdk, txt, s.expected)
		}
	}
}

func TestSdkList(t *testing.T) {
	if SdksList() != [4]string{"Java", "Python", "Go", "SCIO"} {
		t.Error("Sdk list mismatch: ", SdksList())
	}
}
