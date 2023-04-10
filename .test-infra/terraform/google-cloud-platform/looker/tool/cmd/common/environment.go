package common

import (
	"fmt"
	"os"
	"strings"

	"github.com/tatsushid/go-prettytable"
)

const (
	lookerDownloadJarsUrl  = "https://apidownload.looker.com/download"
	defaultLookerNamespace = "looker"

	defaultGcmKeySecretId = "gcm-key"
	// See https://cloud.google.com/looker/docs/changing-encryption-keys#set_new_environment_variables
	defaultGcmKeySecretDataKey = "LKR_MASTER_KEY_ENV"

	defaultLookerDatabaseCredentialsSecretId = "looker-database-credentials"

	// See https://cloud.google.com/looker/docs/migrating-looker-backend-db-to-mysql#create_a_database_credentials_file
	defaultLookerDatabaseCredentialsDataKey = "LOOKER_DB"

	// Aligns with `auth.existingSecret` https://github.com/bitnami/charts/tree/main/bitnami/mysql
	defaultBitamiMySqlCredentialsSecretId   = "bitami-mysql-credentials"
	defaultBitamiRootPasswordDataKey        = "mysql-root-password"
	defaultBitamiReplicationPasswordDataKey = "mysql-replication-password"
	defaultBitamiMySqlPasswordDataKey       = "mysql-password"
)

var (
	GcmKeySecretId      EnvironmentVariable = "GCM_KEY_SECRET_ID"
	GcmKeySecretDataKey EnvironmentVariable = "GCM_KEY_SECRET_DATA_KEY"

	LookerDatabaseCredentialsSecretId EnvironmentVariable = "LOOKER_DATABASE_CREDENTIALS_SECRET_ID"
	LookerDatabaseCredentialsDataKey  EnvironmentVariable = "LOOKER_DATABASE_CREDENTIALS_SECRET_DATA_KEY"

	BitamiMySqlCredentialsSecretId   EnvironmentVariable = "BITAMI_MYSQL_CREDENTIALS_SECRET_Id"
	BitamiRootPasswordDataKey        EnvironmentVariable = "BITAMI_ROOT_PASSWORD_DATA_KEY"
	BitamiReplicationPasswordDataKey EnvironmentVariable = "BITAMI_REPLICATION_PASSWORD_DATA_KEY"
	BitamiMySqlPasswordDataKey       EnvironmentVariable = "BITAMI_MYSQL_PASSWORD_DATA_KEY"

	LookerJarURL    EnvironmentVariable = "LOOKER_JAR_URL"
	LookerNamespace EnvironmentVariable = "LOOKER_NAMESPACE"
	LookerVersion   EnvironmentVariable = "LOOKER_VERSION"
)

func init() {
	if err := defaults(); err != nil {
		panic(err)
	}
}

func defaults() error {
	if err := GcmKeySecretId.Default(defaultGcmKeySecretId); err != nil {
		return err
	}
	if err := GcmKeySecretDataKey.Default(defaultGcmKeySecretDataKey); err != nil {
		return err
	}

	if err := LookerDatabaseCredentialsSecretId.Default(defaultLookerDatabaseCredentialsSecretId); err != nil {
		return err
	}
	if err := LookerDatabaseCredentialsDataKey.Default(defaultLookerDatabaseCredentialsDataKey); err != nil {
		return err
	}

	if err := BitamiMySqlCredentialsSecretId.Default(defaultBitamiMySqlCredentialsSecretId); err != nil {
		return err
	}
	if err := BitamiRootPasswordDataKey.Default(defaultBitamiRootPasswordDataKey); err != nil {
		return err
	}
	if err := BitamiReplicationPasswordDataKey.Default(defaultBitamiReplicationPasswordDataKey); err != nil {
		return err
	}
	if err := BitamiMySqlPasswordDataKey.Default(defaultBitamiMySqlPasswordDataKey); err != nil {
		return err
	}

	if err := LookerJarURL.Default(lookerDownloadJarsUrl); err != nil {
		return err
	}
	if err := LookerNamespace.Default(defaultLookerNamespace); err != nil {
		return err
	}

	return nil
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
