// Package version contains build identity information for the kirocc binary.
package version

import "fmt"

// These values are replaced by release builds with -ldflags.
var (
	Version = "dev"
	Commit  = "unknown"
	BuiltAt = "unknown"
)

// String returns a concise, log-friendly build identity.
func String() string {
	return fmt.Sprintf("%s (%s, built %s)", Version, Commit, BuiltAt)
}
