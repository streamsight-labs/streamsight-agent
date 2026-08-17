package main

import (
	"errors"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"os"
	"runtime"
	"runtime/debug"

	"kafka-metrics-agent/internal/agent"
	"kafka-metrics-agent/internal/config"
)

// version is injected at build time via
// -ldflags "-X main.version=$(git describe --tags --always --dirty)".
var version = "dev"

func main() {
	// The only os.Exit in the program: everything else returns an error so
	// deferred cleanup -- flushing the exporter, closing the Kafka client --
	// actually runs.
	if err := run(); err != nil {
		slog.Error("agent failed", "error", err)
		os.Exit(1)
	}
}

func run() error {
	// ContinueOnError rather than flag.CommandLine: the default ExitOnError
	// calls os.Exit(2) from inside Parse, adding a second exit path.
	fs := flag.NewFlagSet("agent", flag.ContinueOnError)
	showVersion := fs.Bool("version", false, "print build information and exit")
	if err := fs.Parse(os.Args[1:]); err != nil {
		// -h is a successful invocation: Parse has already written the usage
		// text, and exiting non-zero would make a container runtime record a
		// crash for someone asking what the flags are.
		if errors.Is(err, flag.ErrHelp) {
			return nil
		}
		return err
	}

	if *showVersion {
		printVersion(os.Stdout)
		return nil
	}

	cfg, err := config.Load()
	if err != nil {
		return err
	}

	a, err := agent.New(cfg, version)
	if err != nil {
		return err
	}
	defer func() {
		if err := a.Close(); err != nil {
			slog.Error("shutdown error", "error", err)
		}
	}()

	return a.Run()
}

// printVersion answers "what exactly is this binary" without a broker, a
// config, or a restart to read the startup log line -- for an operator and for
// the CI check that the release ldflag survived into the image.
func printVersion(w io.Writer) {
	fmt.Fprintf(w, "kafka-metrics-agent %s\n", version)
	fmt.Fprintf(w, "go: %s\n", runtime.Version())
	fmt.Fprintf(w, "platform: %s/%s\n", runtime.GOOS, runtime.GOARCH)

	info, ok := debug.ReadBuildInfo()
	if !ok {
		return
	}
	// The VCS stamp is best-effort: the toolchain records it only when the build
	// ran inside the work tree with the VCS binary available, true of `make
	// build` and false inside the golang:alpine builder image. A missing stamp
	// is normal, so it is omitted rather than reported as unknown.
	for _, s := range info.Settings {
		switch s.Key {
		case "vcs", "vcs.revision", "vcs.time", "vcs.modified":
			fmt.Fprintf(w, "%s: %s\n", s.Key, s.Value)
		}
	}
}
