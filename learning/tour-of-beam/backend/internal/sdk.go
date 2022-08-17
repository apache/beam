package internal

type Sdk string

const (
	SDK_UNDEFINED Sdk = ""
	SDK_GO        Sdk = "Go"
	SDK_PYTHON    Sdk = "Python"
	SDK_JAVA      Sdk = "Java"
	SDK_SCIO      Sdk = "SCIO"
)

func (s Sdk) String() string {
	return string(s)
}

// Parse sdk from string names, f.e. "Java" -> Sdk.GO_JAVA
// Returns SDK_UNDEFINED on error
func FromString(s string) Sdk {
	switch s {
	case "Go":
		return SDK_GO
	case "Python":
		return SDK_PYTHON
	case "Java":
		return SDK_JAVA
	case "SCIO":
		return SDK_SCIO
	default:
		return SDK_UNDEFINED
	}
}

func SdksList() [4]string {
	return [4]string{"Java", "Python", "Go", "SCIO"}
}
