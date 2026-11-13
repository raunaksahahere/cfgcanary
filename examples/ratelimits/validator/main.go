// Command validator is the consumer-parser validator for the ratelimits
// example, built for WASI (ABI cfgcanary-validator/v1):
//
//	GOOS=wasip1 GOARCH=wasm go build -o validator.wasm ./examples/ratelimits/validator
//
// It reads the payload on stdin, runs the consumer's real Parse function and
// prints {"ok":bool,"errors":[...]}.
package main

import (
	"encoding/json"
	"io"
	"os"

	"github.com/raunaksahahere/cfgcanary/examples/ratelimits"
)

func main() {
	type result struct {
		OK     bool     `json:"ok"`
		Errors []string `json:"errors"`
	}
	res := result{OK: true, Errors: []string{}}
	payload, err := io.ReadAll(os.Stdin)
	if err == nil {
		_, err = ratelimits.Parse(payload)
	}
	if err != nil {
		res = result{OK: false, Errors: []string{err.Error()}}
	}
	_ = json.NewEncoder(os.Stdout).Encode(res)
}
