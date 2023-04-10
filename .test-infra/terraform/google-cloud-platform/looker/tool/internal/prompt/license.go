package prompt

import (
	"context"
	"fmt"

	"github.com/apache/beam/testinfra/terraform/googlecloudplatform/looker/internal/model"
	"github.com/manifoldco/promptui"
)

var (
	lookerLicensePrompt = promptui.Prompt{
		Label:    "Looker License",
		Validate: validateRequiredString,
		Mask:     '*',
	}
	lookerLicenseEmailPrompt = promptui.Prompt{
		Label:    "Looker License Email",
		Validate: validateRequiredString,
	}
)

func License(ctx context.Context, license *model.License, confirmMessage string) (*model.License, error) {
	ctx, cancelFn := context.WithCancel(ctx)
	defer cancelFn()
	errChan := make(chan error)
	resultChan := make(chan *model.License)
	if license == nil {
		license = &model.License{}
	}
	go func() {
		for {
			var err error
			license.Key, err = runPrompt(ctx, lookerLicensePrompt)
			if err != nil {
				errChan <- err
				return
			}

			license.Email, err = runPrompt(ctx, lookerLicenseEmailPrompt)
			if err != nil {
				errChan <- err
			}
			lookerLicenseEmailPrompt.Default = license.Email

			yamlStr, err := license.SecureString()
			if err != nil {
				errChan <- err
				return
			}
			fmt.Println(yamlStr)
			result := confirm(confirmMessage)
			switch result {
			case ok:
				resultChan <- license
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
		case err := <-errChan:
			return nil, err
		case result := <-resultChan:
			return result, nil
		case <-ctx.Done():
			return nil, cancelError
		}
	}
}
