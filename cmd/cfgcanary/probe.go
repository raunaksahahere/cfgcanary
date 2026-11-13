package main

import (
	"net/http"
	"time"

	"github.com/spf13/cobra"
)

// newProbeCmd is a container healthcheck helper (the runtime image has no
// shell or curl): exit 0 when the URL answers 200.
func newProbeCmd() *cobra.Command {
	return &cobra.Command{
		Use: "doctor-probe URL", Hidden: true, Args: cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			c := &http.Client{Timeout: 2 * time.Second}
			resp, err := c.Get(args[0])
			if err != nil {
				return fail(exitUnavailable, "%v", err)
			}
			resp.Body.Close()
			if resp.StatusCode != http.StatusOK {
				return fail(exitUnavailable, "HTTP %d", resp.StatusCode)
			}
			return nil
		},
	}
}
