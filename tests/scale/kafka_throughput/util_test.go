package kafka_throughput

import (
	"context"
	"os/exec"
)

// runCmd is a thin shell helper isolated in its own file so the rest of the package
// stays free of os/exec imports.
func runCmd(ctx context.Context, name string, args ...string) (string, error) {
	out, err := exec.CommandContext(ctx, name, args...).CombinedOutput()
	return string(out), err
}
