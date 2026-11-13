// Package buildinfo holds version information set at link time:
//
//	-X github.com/raunaksahahere/cfgcanary/internal/buildinfo.Version=0.1.0
package buildinfo

import (
	"runtime"
	"runtime/debug"
)

var (
	Version = "0.1.0-dev"
	Commit  = ""
	Date    = ""
)

// Info returns version, commit, date and Go version, falling back to VCS
// information embedded by the Go toolchain for `go install` builds.
func Info() (version, commit, date, goVersion string) {
	version, commit, date = Version, Commit, Date
	if bi, ok := debug.ReadBuildInfo(); ok {
		for _, s := range bi.Settings {
			switch s.Key {
			case "vcs.revision":
				if commit == "" && len(s.Value) >= 12 {
					commit = s.Value[:12]
				}
			case "vcs.time":
				if date == "" {
					date = s.Value
				}
			}
		}
		if version == "0.1.0-dev" && bi.Main.Version != "" && bi.Main.Version != "(devel)" {
			version = bi.Main.Version
		}
	}
	return version, commit, date, runtime.Version()
}
