// Cross-field constraints that JSON Schema cannot express compactly.
import "list"

service: =~"^[a-z][a-z0-9-]*$"

// A burst may be at most ten times the sustained rate.
limits: [...{rps: int, burst: int, _burst_ok: true & (burst <= rps*10)}]

// Feature names are unique.
features: [...{name: string}]
_names: [for f in features {f.name}]
_names_unique: true & list.UniqueItems(_names)
