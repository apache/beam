package jars

import (
	"encoding/json"
	"fmt"
	"os"
	"path"
	"path/filepath"

	"github.com/apache/beam/testinfra/terraform/googlecloudplatform/looker/cmd/common"
	"github.com/apache/beam/testinfra/terraform/googlecloudplatform/looker/internal/jars"
	"github.com/apache/beam/testinfra/terraform/googlecloudplatform/looker/internal/model"
	"github.com/apache/beam/testinfra/terraform/googlecloudplatform/looker/internal/prompt"
	"github.com/spf13/cobra"
	"sigs.k8s.io/controller-runtime/pkg/manager/signals"
)

const (
	defaultDirectory = "."
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

	optionalVars = []common.EnvironmentVariable{
		common.LookerVersion,
	}
	requiredVars = []common.EnvironmentVariable{
		common.LookerJarURL,
	}
)

func init() {
	Command.AddCommand(metadataCmd, downloadCmd)
	common.AddPrintEnvironmentFlag(metadataCmd)
	common.AddPrintEnvironmentFlag(downloadCmd)
}

func preRunE(_ *cobra.Command, _ []string) error {
	return common.Missing(requiredVars[0], requiredVars[1:]...)
}

func downloadE(cmd *cobra.Command, args []string) error {
	ctx := cmd.Context()
	if common.PrintEnvironmentFlag {
		return common.PrintEnvironment(optionalVars, requiredVars)
	}
	dir, err := downloadDir(args)
	if err != nil {
		return err
	}
	confirmationMessage := fmt.Sprintf("Are you sure you want to download jars to %s", dir)
	license, err := prompt.License(signals.SetupSignalHandler(), nil, confirmationMessage)
	if prompt.IsCancelErr(err) {
		return nil
	}

	if err != nil {
		return err
	}

	if common.DryRunFlag {
		fmt.Println("dry run mode, not saving")
		return nil
	}

	req, err := metadataRequest(license)
	if err != nil {
		return err
	}

	resp, err := req.Do(ctx)
	if err != nil {
		return err
	}

	if err := os.MkdirAll(dir, 0755); err != nil {
		return err
	}

	lookerJar, err := os.Create(path.Join(dir, resp.VersionText))
	if err != nil {
		return err
	}

	dependenciesJar, err := os.Create(path.Join(dir, resp.DepDisplayFile))
	if err != nil {
		return err
	}

	fmt.Printf("downloading %s...", resp.VersionText)
	if err = resp.LookerJar(ctx, lookerJar); err != nil {
		return err
	}
	fmt.Print("done\n")

	fmt.Printf("downloading %s...", resp.DepDisplayFile)
	if err = resp.LookerDependencyJar(ctx, dependenciesJar); err != nil {
		return err
	}
	fmt.Print("done\n")

	fmt.Printf("downloaded jars to %s\n", dir)
	return nil
}

func metadataE(cmd *cobra.Command, args []string) error {
	ctx := cmd.Context()
	if common.PrintEnvironmentFlag {
		return common.PrintEnvironment(optionalVars, requiredVars)
	}

	confirmationMessage := "Do you want to request Looker Jar metadata?"

	lic, err := prompt.License(signals.SetupSignalHandler(), nil, confirmationMessage)
	if prompt.IsCancelErr(err) {
		return nil
	}

	if err != nil {
		return err
	}

	if common.DryRunFlag {
		fmt.Println("dry run mode, not saving")
		return nil
	}

	req, err := metadataRequest(lic)
	if err != nil {
		return err
	}

	resp, err := req.Do(ctx)
	if err != nil {
		return err
	}

	return json.NewEncoder(os.Stdout).Encode(resp)
}

func downloadDir(args []string) (string, error) {
	dir := defaultDirectory
	if len(args) > 0 {
		dir = args[0]
	}
	return filepath.Abs(dir)
}

func metadataRequest(license *model.License) (*jars.MetadataRequest, error) {
	version, err := versionFromEnvironment()
	if err != nil {
		return nil, err
	}
	return &jars.MetadataRequest{
		LookerJarUrl: common.LookerJarURL.Value(),
		LicenseKey:   license.Key,
		Email:        license.Email,
		Version:      version,
	}, nil
}

func versionFromEnvironment() (*jars.Version, error) {
	if common.LookerVersion.Missing() {
		return nil, nil
	}
	return jars.ParseVersion(common.LookerVersion.Value())
}
