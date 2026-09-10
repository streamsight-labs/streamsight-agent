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

	"github.com/streamsight-labs/streamsight-agent/internal/agent"
	"github.com/streamsight-labs/streamsight-agent/internal/config"
)

// version is injected at build time via
// -ldflags "-X main.version=$(git describe --tags --always --dirty)".
var version = "dev"

func main() {
	// The only os.Exit in the program: everything else returns an error so
	// deferred cleanup -- flushing the exporter, closing the Kafka client --
	// actually runs.
	if err := run(); err != nil {
		fatal(os.Stderr, "agent failed", err)
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
			fatal(os.Stderr, "shutdown error", err)
		}
	}()

	return a.Run()
}

// fatal reports a failure on its way out of main. It builds its own handler
// rather than calling slog.Error, because the package-level default logger uses
// the log package's format: a run that fails after startup would then print the
// agent's own lines in one shape and this one in another, in the same stderr
// stream, which is the first thing anyone reading a pasted log notices. The
// agent's configured logger is not reachable here -- two of the three failures
// this reports (an unloadable config, an agent that could not be built) happen
// before one exists -- so the handler is mirrored instead of shared. Level is
// left at the default: every line from here is ERROR, which no accepted
// LOG_LEVEL suppresses.
func fatal(w io.Writer, msg string, err error) {
	slog.New(slog.NewTextHandler(w, nil)).Error(msg, "error", err)
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
