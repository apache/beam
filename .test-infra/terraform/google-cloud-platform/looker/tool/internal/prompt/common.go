package prompt

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/apache/beam/testinfra/terraform/googlecloudplatform/looker/internal/k8s"
	"github.com/manifoldco/promptui"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

var (
	ok     = "✅ ok"
	cancel = "❌ cancel"
	edit   = "✏ edit"

	cancelError = errors.New("cancel")
)

func IsCancelErr(err error) bool {
	if err == nil {
		return false
	}
	return err.Error() == cancelError.Error()
}

func ConfirmationMessage(kind string, meta metav1.ObjectMeta) string {
	return fmt.Sprintf("Are you sure you want to save %s/%s in namespace %s", kind, meta.Name, meta.Namespace)
}

func OperationMessage(kind string, op k8s.Operation, meta metav1.ObjectMeta) string {
	return fmt.Sprintf("%s %s/%s in namespace %s", op, kind, meta.Name, meta.Namespace)
}

func validateRequiredString(input string) error {
	if input != "" {
		return nil
	}
	return fmt.Errorf("input is empty but requiredVars")
}

func runPrompt(ctx context.Context, prompt promptui.Prompt) (string, error) {
	ctx, cancelFn := context.WithCancel(ctx)
	defer cancelFn()
	resultChan := make(chan string)
	go func() {
		for {
			result, err := prompt.Run()
			if isPromptInterrupt(err) {
				cancelFn()
				return
			}
			if err == nil {
				resultChan <- result
				return
			}
			fmt.Println(err)
		}
	}()
	for {
		select {
		case result := <-resultChan:
			return result, nil
		case <-ctx.Done():
			return "", cancelError
		}
	}
}

func YesNo(message string) bool {
	prompt := promptui.Prompt{
		Label:     message,
		IsConfirm: true,
	}
	for {
		result, err := prompt.Run()
		if result == "" {
			err = nil
			result = "N"
		}
		if err != nil {
			continue
		}
		return strings.ToLower(result) == "y"
	}
}

func confirm(message string) string {
	prompt := promptui.Select{
		Label: message,
		Items: []string{ok, cancel, edit},
	}
	for {
		_, choice, err := prompt.Run()
		if err != nil {
			continue
		}
		return choice
	}
}

func isPromptInterrupt(err error) bool {
	if err == nil {
		return false
	}
	return err.Error() == "^C"
}
