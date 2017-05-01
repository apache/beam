package exec

import (
	"context"
	"fmt"
)

// Execute executes contained graph segment. At least one root is required.
func Execute(ctx context.Context, units []Unit) error {
	var roots []Root
	for _, u := range units {
		if root, ok := u.(Root); ok {
			roots = append(roots, root)
		}
	}
	if len(roots) == 0 {
		return fmt.Errorf("no roots to execute")
	}
	for _, root := range roots {
		if err := root.Up(ctx); err != nil {
			return err
		}
	}
	for _, root := range roots {
		if err := root.Process(ctx); err != nil {
			return err
		}
	}
	for _, root := range roots {
		if err := root.Down(ctx); err != nil {
			return err
		}
	}
	return nil
}
