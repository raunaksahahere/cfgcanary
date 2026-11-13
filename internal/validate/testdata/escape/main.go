// escape tries everything a hostile validator might: read files, read the
// environment, open a socket, read the clock. It reports what it managed.
package main

import (
	"encoding/json"
	"net"
	"os"
	"time"
)

func main() {
	var got []string
	for _, p := range []string{"/etc/passwd", "/", ".", "/proc/self/environ", "/tmp/x"} {
		if _, err := os.ReadFile(p); err == nil {
			got = append(got, "read:"+p)
		}
		if _, err := os.ReadDir(p); err == nil {
			got = append(got, "readdir:"+p)
		}
	}
	if err := os.WriteFile("/tmp/cfgcanary-escape", []byte("x"), 0o600); err == nil {
		got = append(got, "write:/tmp")
	}
	if len(os.Environ()) > 0 {
		got = append(got, "env")
	}
	if len(os.Args) > 1 {
		got = append(got, "args")
	}
	if c, err := net.Dial("tcp", "127.0.0.1:80"); err == nil {
		c.Close()
		got = append(got, "dial")
	}
	// net.Listen is not probed: on wasip1 Go implements it with an in-module
	// fake network that never reaches the host.
	if c, err := net.Dial("tcp", "example.com:443"); err == nil {
		c.Close()
		got = append(got, "dial-dns")
	}
	// The host clock must not leak: wazero's default is a fixed fake time.
	if time.Now().Year() >= 2024 {
		got = append(got, "clock")
	}
	if got == nil {
		got = []string{}
	}
	_ = json.NewEncoder(os.Stdout).Encode(map[string]any{"ok": len(got) == 0, "errors": got})
}
