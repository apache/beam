package jars

import (
	"encoding/json"
	"fmt"
	"os"
	"strconv"
	"strings"

	"github.com/apache/beam/testinfra/terraform/googlecloudplatform/looker/cmd/common"
	"github.com/apache/beam/testinfra/terraform/googlecloudplatform/looker/internal/jars"
	"github.com/apache/beam/testinfra/terraform/googlecloudplatform/looker/internal/secret"
	"github.com/manifoldco/promptui"
	"github.com/spf13/cobra"
)

var (
	Command = &cobra.Command{
		Use:               "jars",
		Short:             "Manage Looker related jars",
		PersistentPreRunE: preRunE,
	}

	metadataCmd = &cobra.Command{
		Use:   "metadata",
		Short: "Show Looker Jar metadata",
		RunE:  metadataE,
	}

	downloadCmd = &cobra.Command{
		Use:   "download [DIRECTORY]",
		Short: "Download Looker Jars",
		RunE:  downloadE,
		Args:  cobra.MaximumNArgs(1),
	}

	prov *secret.Provision
	req  *jars.MetadataRequest

	optionalVars = []common.EnvironmentVariable{
		common.LookerVersion,
	}
	requiredVars = []common.EnvironmentVariable{
		common.ProjectId,
		common.LookerLicenseSecretId,
		common.LookerJarURL,
	}
)

func init() {
	Command.AddCommand(metadataCmd, downloadCmd)
	common.AddPrintEnvironmentFlag(metadataCmd)
	common.AddPrintEnvironmentFlag(downloadCmd)
}

func preRunE(cmd *cobra.Command, args []string) error {
	ctx := cmd.Context()
	if err := common.Missing(requiredVars[0], requiredVars[1:]...); err != nil {
		return err
	}
	if err := common.PreRun(cmd, args); err != nil {
		return err
	}
	if err := common.SecretReader.ReadIntoLatestVersion(ctx, common.ProjectId.Value(), common.LookerLicenseSecretId.Value(), &prov); err != nil {
		return err
	}
	req = &jars.MetadataRequest{
		LookerJarUrl: common.LookerJarURL.Value(),
		LicenseKey:   prov.LicenseKey,
		Email:        prov.User.Email,
	}
	version, err := versionFromEnvironment()
	if err != nil {
		return err
	}
	req.Version = version

	return nil
}

func downloadE(cmd *cobra.Command, args []string) error {
	if common.PrintEnvironmentFlag {
		return common.PrintEnvironment(optionalVars, requiredVars)
	}
	ctx := cmd.Context()
	resp, err := req.Do(ctx)
	if err != nil {
		return err
	}
	confirmDownload := promptui.Prompt{
		Label:     fmt.Sprintf("Are you sure you want to download %s and %s?", resp.VersionText, resp.DepDisplayFile),
		IsConfirm: true,
	}
	ok, err := confirmDownload.Run()
	if err != nil {
		return nil
	}
	if strings.ToLower(ok) != "y" {
		return nil
	}
	lookerJar, err := os.Create(resp.VersionText)
	if err != nil {
		return fmt.Errorf("error creating file %s, error %w", resp.VersionText, err)
	}
	lookerDepencencyJar, err := os.Create(resp.DepDisplayFile)
	if err != nil {
		return fmt.Errorf("error creating file %s, error %w", resp.DepDisplayFile, err)
	}
	fmt.Printf("downloading %s and %s...\n", resp.VersionText, resp.DepDisplayFile)
	if err := resp.LookerJar(ctx, lookerJar); err != nil {
		return err
	}
	if err := resp.LookerDependencyJar(ctx, lookerDepencencyJar); err != nil {
		return err
	}
	return nil
}

func metadataE(cmd *cobra.Command, _ []string) error {
	if common.PrintEnvironmentFlag {
		return common.PrintEnvironment(optionalVars, requiredVars)
	}
	ctx := cmd.Context()
	resp, err := req.Do(ctx)
	if err != nil {
		return err
	}
	return json.NewEncoder(os.Stdout).Encode(resp)
}

func versionFromEnvironment() (*jars.Version, error) {
	if common.LookerVersion.Missing() {
		return nil, nil
	}
	split := strings.Split(common.LookerVersion.Value(), ".")
	if len(split) != 2 {
		return nil, fmt.Errorf("error parsing version from environment variable %s, expected <Major>.<Minor>", common.LookerVersion.KeyValue())
	}
	major, err := strconv.Atoi(split[0])
	if err != nil {
		return nil, fmt.Errorf("error parsing version from environment variable %s, expected <Major>.<Minor>", common.LookerVersion.KeyValue())
	}
	minor, err := strconv.Atoi(split[1])
	if err != nil {
		return nil, fmt.Errorf("error parsing version from environment variable %s, expected <Major>.<Minor>", common.LookerVersion.KeyValue())
	}
	return &jars.Version{
		Major: major,
		Minor: minor,
	}, nil
}
