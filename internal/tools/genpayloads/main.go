// Command genpayloads regenerates examples/ratelimits/payloads.
package main

import (
	"fmt"
	"os"
	"strings"

	"github.com/raunaksahahere/cfgcanary/internal/testenv"
)

func main() {
	dir := os.Args[1]
	w := func(name string, b []byte) { _ = os.WriteFile(dir+"/"+name, append(b, '\n'), 0o644) }
	w("v1-baseline.json", testenv.Payload(40, 250))
	w("v2-good.json", testenv.Payload(44, 250))
	w("v3-harmful-timeout.json", testenv.Payload(44, 20))
	w("bad-oversized.json", testenv.Payload(600, 250))
	w("bad-over-cardinality.json", testenv.Payload(201, 250))
	w("bad-doubled.json", testenv.Payload(100, 250))
	blank := strings.Replace(string(testenv.Payload(44, 250)), `"service": "checkout"`, `"service": ""`, 1)
	w("bad-blank-service.json", []byte(blank))
	rule := strings.Replace(string(testenv.Payload(44, 250)), `"rule": "region == eu"`, `"rule": "region =~ eu"`, 1)
	w("bad-parser-rule.json", []byte(rule))
	w("bad-malformed.json", []byte(`{"schema_version": 1, "service": "checkout", "timeout_ms": 250, "features": [`))
	fmt.Println("ok")
}
