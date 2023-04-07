package adminconfig

import (
	"fmt"

	"github.com/apache/beam/testinfra/terraform/googlecloudplatform/looker/cmd/common"
	"github.com/apache/beam/testinfra/terraform/googlecloudplatform/looker/internal/secret"
	"github.com/manifoldco/promptui"
	"github.com/spf13/cobra"
)

var (
	Command = &cobra.Command{
		Use:     "adminconfig",
		Short:   "Store provision.yaml admin user file in Google Secret Manager",
		PreRunE: preRunE,
		RunE:    runE,
	}

	requiredVars = []common.EnvironmentVariable{
		common.ProjectId,
		common.LookerLicenseSecretId,
	}

	confirm = promptui.Prompt{
		Label: fmt.Sprintf("Would you like to save this in projects/%s/secrets/%s [Y/N]", common.ProjectId.Value(), common.LookerLicenseSecretId.Value()),
	}

	lookerLicensePrompt = promptui.Prompt{
		Label:    "Looker License",
		Validate: validateRequiredString,
		Mask:     '*',
	}
	lookerLicenseEmailPrompt = promptui.Prompt{
		Label:    "Looker License Email",
		Validate: validateRequiredString,
	}
	firstNamePrompt = promptui.Prompt{
		Label:    "First name",
		Validate: validateRequiredString,
	}
	lastNamePrompt = promptui.Prompt{
		Label:    "Last name",
		Validate: validateRequiredString,
	}
	hostPrompt = promptui.Prompt{
		Label:   "Host URL",
		Default: "http://localhost",
	}
	passwordPrompt = promptui.Prompt{
		Label:    "Password",
		Validate: validateRequiredString,
		Mask:     '*',
	}
)

func init() {
	common.AddPrintEnvironmentFlag(Command)
}

func preRunE(cmd *cobra.Command, args []string) error {
	if common.PrintEnvironmentFlag {
		return nil
	}
	if err := common.Missing(requiredVars[0], requiredVars[1:]...); err != nil {
		return err
	}

	return common.PreRun(cmd, args)
}

func runE(cmd *cobra.Command, _ []string) error {
	ctx := cmd.Context()

	if common.PrintEnvironmentFlag {
		return common.PrintEnvironment(nil, requiredVars)
	}

	provision, err := prompt(nil)
	if err != nil {
		return err
	}

	return common.SecretWriter.Write(ctx, common.ProjectId.Value(), common.LookerLicenseSecretId.Value(), provision)
}

func prompt(provision *secret.Provision) (*secret.Provision, error) {
	if provision == nil {
		provision = &secret.Provision{
			User: &secret.User{},
		}
	}
	for {
		provision.LicenseKey = runPrompt(lookerLicensePrompt)

		provision.HostUrl = runPrompt(hostPrompt)
		hostPrompt.Default = provision.HostUrl

		provision.User.Email = runPrompt(lookerLicenseEmailPrompt)
		lookerLicenseEmailPrompt.Default = provision.User.Email

		provision.User.FirstName = runPrompt(firstNamePrompt)
		firstNamePrompt.Default = provision.User.FirstName

		provision.User.LastName = runPrompt(lastNamePrompt)
		lastNamePrompt.Default = provision.User.LastName

		provision.User.Password = runPrompt(passwordPrompt)
		yamlStr, err := provision.SecureString()
		if err != nil {
			return nil, err
		}
		fmt.Println(yamlStr)
		ok, err := confirm.Run()
		if err != nil {
			continue
		}
		if ok == "Y" {
			return provision, nil
		}
	}
}

func validateRequiredString(input string) error {
	if input != "" {
		return nil
	}
	return fmt.Errorf("input is empty but requiredVars")
}

func runPrompt(prompt promptui.Prompt) string {
	for {
		result, err := prompt.Run()
		if err == nil {
			return result
		}
		fmt.Println(err)
	}
}
