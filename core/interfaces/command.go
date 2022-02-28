package interfaces

import "github.com/spf13/cobra"

type BuildCommand interface {
	Build() *cobra.Command
}
