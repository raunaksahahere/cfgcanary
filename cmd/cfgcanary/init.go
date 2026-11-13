package main

import (
	"os"
	"path/filepath"

	"github.com/spf13/cobra"
)

const initSpec = `# cfgcanary config spec. Register with: cfgcanary configs register -f spec.yaml
name: %NAME%
description: Example structured runtime config
format: json
# fail_static keeps serving the last-known-good value when the control plane is
# unreachable; fail_closed marks it unusable after max_staleness.
failure_mode: FAILURE_MODE_FAIL_STATIC
max_staleness: 600s
retain_versions: 20
validation:
  limits:
    max_bytes: 65536
    max_items: {"/features": 200}
  json_schema_file: schema.json
  relative:
    max_size_ratio: 2
    max_items_ratio: {"/features": 2}
environments:
  prod:
    rings:
      - {name: canary, cumulative_bps: 1000}
      - {name: early, cumulative_bps: 3000}
      - {name: broad, cumulative_bps: 9000}
    min_instances: 3
    acceptance_threshold: 0.9
    promotion: PROMOTION_MANUAL
    gate:
      mode: GATE_MODE_NONE
      min_bake: 60s
`

const initSchema = `{
  "$schema": "https://json-schema.org/draft/2020-12/schema",
  "type": "object",
  "additionalProperties": false,
  "required": ["features"],
  "properties": {
    "features": {
      "type": "array",
      "items": {
        "type": "object",
        "additionalProperties": false,
        "required": ["name", "enabled"],
        "properties": {"name": {"type": "string", "minLength": 1}, "enabled": {"type": "boolean"}}
      }
    }
  }
}
`

const initPayload = `{"features": [{"name": "new-checkout", "enabled": false}]}
`

func newInitCmd() *cobra.Command {
	var dir, name string
	cmd := &cobra.Command{
		Use:   "init",
		Short: "Scaffold a config spec, JSON Schema and first payload",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			if err := os.MkdirAll(dir, 0o755); err != nil {
				return err
			}
			files := map[string]string{"spec.yaml": replaceName(initSpec, name), "schema.json": initSchema, "payload.json": initPayload}
			for f := range files {
				p := filepath.Join(dir, f)
				if _, err := os.Stat(p); err == nil {
					return fail(exitPrecondition, "%s already exists; refusing to overwrite", p)
				}
			}
			for f, content := range files {
				if err := os.WriteFile(filepath.Join(dir, f), []byte(content), 0o644); err != nil {
					return err
				}
			}
			printf("created %s/{spec.yaml,schema.json,payload.json}\nnext:\n  cfgcanary configs register -f %s/spec.yaml\n  cfgcanary validate %s -f %s/payload.json\n  cfgcanary versions submit %s -f %s/payload.json\n",
				dir, dir, name, dir, name, dir)
			return nil
		},
	}
	cmd.Flags().StringVar(&dir, "dir", "cfgcanary", "output directory")
	cmd.Flags().StringVar(&name, "name", "features", "config name")
	return cmd
}

func replaceName(s, name string) string {
	out := []byte{}
	for i := 0; i < len(s); i++ {
		if i+6 <= len(s) && s[i:i+6] == "%NAME%" {
			out = append(out, name...)
			i += 5
			continue
		}
		out = append(out, s[i])
	}
	return string(out)
}
