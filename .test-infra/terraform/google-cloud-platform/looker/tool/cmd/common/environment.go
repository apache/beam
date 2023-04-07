package common

import (
	"fmt"
	"os"
	"strings"

	"github.com/spf13/cobra"
	"github.com/tatsushid/go-prettytable"
)

const (
	lookerDownloadJarsUrl = "https://apidownload.looker.com/download"
)

var (
	GcmSecretId           EnvironmentVariable = "GCM_SECRET_ID"
	LookerJarURL          EnvironmentVariable = "LOOKER_JAR_URL"
	LookerVersion         EnvironmentVariable = "LOOKER_VERSION"
	LookerLicenseSecretId EnvironmentVariable = "LOOKER_LICENSE_SECRET_ID"
	ProjectId             EnvironmentVariable = "PROJECT_ID"
)

func init() {
	if err := LookerJarURL.Default(lookerDownloadJarsUrl); err != nil {
		panic(err)
	}
}

type EnvironmentVariable string

func (v EnvironmentVariable) Default(value string) error {
	if v.Missing() {
		return os.Setenv((string)(v), value)
	}
	return nil
}

func (v EnvironmentVariable) Missing() bool {
	return v.Value() == ""
}

func (v EnvironmentVariable) Value() string {
	return os.Getenv((string)(v))
}

func (v EnvironmentVariable) KeyValue() string {
	return fmt.Sprintf("%s=%s", (string)(v), v.Value())
}

func Missing(variable EnvironmentVariable, addl ...EnvironmentVariable) error {
	var missing []string
	addl = append([]EnvironmentVariable{variable}, addl...)
	for _, v := range addl {
		if v.Missing() {
			missing = append(missing, v.KeyValue())
		}
	}
	if len(missing) > 0 {
		return fmt.Errorf("variables empty but expected from environment: %s", strings.Join(missing, "; "))
	}
	return nil
}

func PrintEnvironment(optional []EnvironmentVariable, required []EnvironmentVariable) error {
	if optional == nil {
		optional = []EnvironmentVariable{}
	}
	if required == nil {
		optional = []EnvironmentVariable{}
	}
	requiredStr := "required"
	optionalStr := "optional"
	vars := map[EnvironmentVariable]bool{}
	for _, v := range optional {
		vars[v] = false
	}
	for _, v := range required {
		vars[v] = true
	}
	tbl, err := prettytable.NewTable([]prettytable.Column{
		{Header: "KEY"},
		{Header: "VALUE"},
		{Header: "REQUIRED/OPTIONAL"},
	}...)
	if err != nil {
		return err
	}
	for k, reqd := range vars {
		reqdVal := optionalStr
		if reqd {
			reqdVal = requiredStr
		}
		if err := tbl.AddRow((string)(k), k.Value(), reqdVal); err != nil {
			return err
		}
	}
	_, err = tbl.Print()
	return err
}

func AddPrintEnvironmentFlag(cmd *cobra.Command) {
	cmd.Flags().BoolVar(&PrintEnvironmentFlag, "env", false, "Print environment variables")
}
