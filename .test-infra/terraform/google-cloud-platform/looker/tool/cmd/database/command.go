package database

import "github.com/spf13/cobra"

var (
	Command = &cobra.Command{
		Use:   "database",
		Short: "Manage database credential information",
	}
)
