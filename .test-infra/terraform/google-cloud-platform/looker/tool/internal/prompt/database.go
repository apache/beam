package prompt

import (
	"context"
	"fmt"
	"regexp"
	"strconv"
	"strings"

	"github.com/apache/beam/testinfra/terraform/googlecloudplatform/looker/internal/model"
	"github.com/manifoldco/promptui"
)

var (
	dialectPrompt = promptui.Prompt{
		Label:    "Dialect",
		Default:  "mysql",
		Validate: validateRequiredString,
	}
	databaseHostPrompt = promptui.Prompt{
		Label:    "Host",
		Default:  "mysql.looker.svc.cluster.local",
		Validate: validateRequiredString,
	}
	databasePasswordPrompt = promptui.Prompt{
		Label:    "Password",
		Validate: validateMySqlPassword,
		Mask:     '*',
	}
	usernamePrompt = promptui.Prompt{
		Label:    "Username",
		Default:  "looker",
		Validate: validateRequiredString,
	}
	databasePrompt = promptui.Prompt{
		Label:    "Database",
		Default:  "looker",
		Validate: validateRequiredString,
	}
	portPrompt = promptui.Prompt{
		Label:    "Port",
		Default:  "3306",
		Validate: validateInteger,
	}
)

func DatabaseCredentials(ctx context.Context, credentials *model.DatabaseCredentials, confirmationMessage string) (*model.DatabaseCredentials, error) {
	ctx, cancelFn := context.WithCancel(ctx)
	defer cancelFn()
	errChan := make(chan error)
	resultChan := make(chan *model.DatabaseCredentials)
	if credentials == nil {
		credentials = &model.DatabaseCredentials{}
	}
	go func() {
		for {
			var err error
			credentials.Dialect, err = runPrompt(ctx, dialectPrompt)
			if err != nil {
				errChan <- err
			}
			dialectPrompt.Default = credentials.Dialect

			credentials.Host, err = runPrompt(ctx, databaseHostPrompt)
			if err != nil {
				errChan <- err
			}
			databaseHostPrompt.Default = credentials.Host

			credentials.Username, err = runPrompt(ctx, usernamePrompt)
			if err != nil {
				errChan <- err
			}
			usernamePrompt.Default = credentials.Username

			credentials.Password, err = runPrompt(ctx, databasePasswordPrompt)
			if err != nil {
				errChan <- err
			}
			databasePasswordPrompt.Default = credentials.Password

			credentials.Database, err = runPrompt(ctx, databasePrompt)
			if err != nil {
				errChan <- err
			}
			databasePrompt.Default = credentials.Database

			credentials.Port, err = runPrompt(ctx, portPrompt)
			if err != nil {
				errChan <- err
			}
			portPrompt.Default = credentials.Port

			credStr, err := credentials.SecureString()
			if err != nil {
				errChan <- err
				return
			}
			fmt.Println(credStr)
			result := confirm(confirmationMessage)
			switch result {
			case ok:
				resultChan <- credentials
				return
			case cancel:
				errChan <- cancelError
				return
			case edit:
				continue
			}
		}
	}()
	for {
		select {
		case result := <-resultChan:
			return result, nil
		case err := <-errChan:
			return nil, err
		case <-ctx.Done():
			return nil, cancelError
		}
	}
}

func validateInteger(s string) error {
	if err := validateRequiredString(s); err != nil {
		return err
	}
	_, err := strconv.Atoi(s)
	return err
}

func validateMySqlPassword(s string) error {
	var errs []string
	p := regexp.MustCompile("[a-zA-Z\\d\\*]")
	if len(s) < 8 || len(s) > 32 {
		errs = append(errs, "not with length requirements [8, 32]")
	}
	if !p.MatchString(s) {
		errs = append(errs, "not within allowed character requirements %s", p.String())
	}
	if len(errs) > 0 {
		return fmt.Errorf("password does not meet mysql requirments: %s", strings.Join(errs, "; "))
	}
	return nil
}
