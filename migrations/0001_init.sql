-- cfgcanary schema, migration 1.
-- PostgreSQL is the single authority for release state (docs/invariants.md I3).

CREATE TABLE configs (
    name          text PRIMARY KEY CHECK (name ~ '^[a-z0-9][a-z0-9._-]{0,62}$'),
    spec          jsonb       NOT NULL,
    spec_revision bigint      NOT NULL DEFAULT 1,
    version_seq   bigint      NOT NULL DEFAULT 0,
    created_at    timestamptz NOT NULL DEFAULT now(),
    updated_at    timestamptz NOT NULL DEFAULT now()
);

-- Content-addressed objects. A row exists before the object is uploaded
-- (state 'pending') so that garbage collection and uploads serialize on it.
CREATE TABLE blobs (
    sha256     text PRIMARY KEY CHECK (sha256 ~ '^[0-9a-f]{64}$'),
    size       bigint      NOT NULL CHECK (size >= 0),
    kind       text        NOT NULL CHECK (kind IN ('payload', 'wasm')),
    state      text        NOT NULL CHECK (state IN ('pending', 'committed')),
    touched_at timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE config_versions (
    config             text        NOT NULL REFERENCES configs (name),
    version            bigint      NOT NULL CHECK (version > 0),
    sha256             text        NOT NULL REFERENCES blobs (sha256),
    size               bigint      NOT NULL,
    description        text        NOT NULL DEFAULT '',
    environments       text[]      NOT NULL,
    created_by         text        NOT NULL,
    created_at         timestamptz NOT NULL DEFAULT now(),
    artifact_payload   bytea       NOT NULL,
    artifact_signature bytea       NOT NULL,
    artifact_key_id    text        NOT NULL,
    purged_at          timestamptz,
    PRIMARY KEY (config, version)
);
CREATE INDEX config_versions_sha ON config_versions (sha256) WHERE purged_at IS NULL;

-- Versions are immutable: only purged_at may ever change.
CREATE FUNCTION config_versions_immutable() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    IF TG_OP = 'DELETE' THEN
        RAISE EXCEPTION 'config versions cannot be deleted';
    END IF;
    IF (NEW.config, NEW.version, NEW.sha256, NEW.size, NEW.environments, NEW.created_by, NEW.created_at,
        NEW.artifact_payload, NEW.artifact_signature, NEW.artifact_key_id)
       IS DISTINCT FROM
       (OLD.config, OLD.version, OLD.sha256, OLD.size, OLD.environments, OLD.created_by, OLD.created_at,
        OLD.artifact_payload, OLD.artifact_signature, OLD.artifact_key_id) THEN
        RAISE EXCEPTION 'config versions are immutable';
    END IF;
    RETURN NEW;
END $$;
CREATE TRIGGER config_versions_immutable BEFORE UPDATE OR DELETE ON config_versions
    FOR EACH ROW EXECUTE FUNCTION config_versions_immutable();

-- Desired state per (config, environment).
CREATE TABLE targets (
    config                text        NOT NULL REFERENCES configs (name),
    environment           text        NOT NULL CHECK (environment ~ '^[a-z0-9][a-z0-9-]{0,30}$'),
    generation            bigint      NOT NULL DEFAULT 0,
    baseline_version      bigint      NOT NULL DEFAULT 0,
    candidate_version     bigint      NOT NULL DEFAULT 0,
    active_ring           integer     NOT NULL DEFAULT -1,
    release_id            text        NOT NULL DEFAULT '',
    previous_good_version bigint      NOT NULL DEFAULT 0,
    frozen                boolean     NOT NULL DEFAULT false,
    frozen_reason         text        NOT NULL DEFAULT '',
    -- Ring policy in force: the active release's frozen policy, otherwise
    -- the policy of the last release. Used to tell agents their ring.
    ring_policy           jsonb       NOT NULL DEFAULT '{}',
    updated_at            timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (config, environment)
);

-- Generations only increase (docs/invariants.md I1), enforced in the database
-- as well as in internal/rollout.
CREATE FUNCTION targets_generation_monotonic() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    IF NEW.generation < OLD.generation THEN
        RAISE EXCEPTION 'target generation cannot decrease (% -> %)', OLD.generation, NEW.generation;
    END IF;
    IF NEW.generation = OLD.generation AND
       (NEW.baseline_version, NEW.candidate_version, NEW.active_ring)
       IS DISTINCT FROM (OLD.baseline_version, OLD.candidate_version, OLD.active_ring) THEN
        RAISE EXCEPTION 'desired state changed without a new generation';
    END IF;
    RETURN NEW;
END $$;
CREATE TRIGGER targets_generation_monotonic BEFORE UPDATE ON targets
    FOR EACH ROW EXECUTE FUNCTION targets_generation_monotonic();

CREATE TABLE releases (
    id                  text PRIMARY KEY,
    config              text        NOT NULL,
    environment         text        NOT NULL,
    version             bigint      NOT NULL,
    baseline_version    bigint      NOT NULL,
    kind                text        NOT NULL DEFAULT '',
    state               text        NOT NULL CHECK (state IN
        ('PENDING', 'ROLLING', 'BAKING', 'HALTED', 'ROLLING_BACK', 'ROLLED_BACK', 'COMPLETE')),
    paused              boolean     NOT NULL DEFAULT false,
    active_ring         integer     NOT NULL DEFAULT -1,
    -- Highest ring that was ever exposed to the candidate.
    exposed_ring        integer     NOT NULL DEFAULT -1,
    reason              text        NOT NULL DEFAULT '',
    message             text        NOT NULL DEFAULT '',
    note                text        NOT NULL DEFAULT '',
    state_version       bigint      NOT NULL DEFAULT 1,
    epoch               bigint      NOT NULL DEFAULT 0,
    first_generation    bigint      NOT NULL DEFAULT 0,
    ring_generation     bigint      NOT NULL DEFAULT 0,
    rollback_generation bigint      NOT NULL DEFAULT 0,
    ring_started_at     timestamptz,
    bake_started_at     timestamptz,
    last_look_at        timestamptz,
    rollback_started_at timestamptz,
    looks_done          integer     NOT NULL DEFAULT 0,
    gate_passed         boolean     NOT NULL DEFAULT false,
    override_used       boolean     NOT NULL DEFAULT false,
    rollback_converged  boolean     NOT NULL DEFAULT false,
    policy              jsonb       NOT NULL,
    rings               jsonb       NOT NULL DEFAULT '[]',
    rollback_summary    jsonb,
    created_by          text        NOT NULL,
    created_at          timestamptz NOT NULL DEFAULT now(),
    updated_at          timestamptz NOT NULL DEFAULT now(),
    finished_at         timestamptz,
    FOREIGN KEY (config, environment) REFERENCES targets (config, environment)
);
-- At most one active release per target.
CREATE UNIQUE INDEX releases_one_active ON releases (config, environment)
    WHERE state IN ('PENDING', 'ROLLING', 'BAKING', 'HALTED', 'ROLLING_BACK');
CREATE INDEX releases_by_target ON releases (config, environment, created_at DESC);

CREATE TABLE instances (
    environment   text        NOT NULL,
    instance_id   text        NOT NULL,
    grp           text        NOT NULL DEFAULT '',
    labels        jsonb       NOT NULL DEFAULT '{}',
    agent_version text        NOT NULL DEFAULT '',
    first_seen    timestamptz NOT NULL DEFAULT now(),
    last_seen     timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (environment, instance_id)
);
CREATE INDEX instances_last_seen ON instances (environment, last_seen);

-- The cohort of a release, frozen at creation; late joiners are appended.
CREATE TABLE release_members (
    release_id  text    NOT NULL REFERENCES releases (id),
    instance_id text    NOT NULL,
    ring        integer NOT NULL,
    late        boolean NOT NULL DEFAULT false,
    PRIMARY KEY (release_id, instance_id)
);
CREATE INDEX release_members_ring ON release_members (release_id, ring);

-- Latest acknowledgement per instance and target. Ordered by generation,
-- never by arrival time.
CREATE TABLE instance_status (
    environment        text        NOT NULL,
    instance_id        text        NOT NULL,
    config             text        NOT NULL,
    generation         bigint      NOT NULL,
    version            bigint      NOT NULL,
    state              text        NOT NULL,
    reason             text        NOT NULL DEFAULT '',
    serving_version    bigint      NOT NULL DEFAULT 0,
    serving_generation bigint      NOT NULL DEFAULT 0,
    updated_at         timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (environment, config, instance_id)
);

-- SDK rejections and crash loops are remembered per version even after the
-- instance's latest status moves on (e.g. it is rolled back).
CREATE TABLE instance_incidents (
    environment text        NOT NULL,
    config      text        NOT NULL,
    version     bigint      NOT NULL,
    instance_id text        NOT NULL,
    kind        text        NOT NULL CHECK (kind IN ('REJECTED', 'CRASH_LOOP')),
    generation  bigint      NOT NULL,
    reason      text        NOT NULL DEFAULT '',
    created_at  timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (environment, config, version, instance_id, kind)
);

CREATE TABLE gate_evaluations (
    id         bigserial PRIMARY KEY,
    release_id text        NOT NULL REFERENCES releases (id),
    ring       integer     NOT NULL,
    look       integer     NOT NULL,
    verdict    text        NOT NULL,
    reason     text        NOT NULL DEFAULT '',
    evidence   jsonb       NOT NULL DEFAULT '{}',
    epoch      bigint      NOT NULL,
    created_at timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX gate_evaluations_release ON gate_evaluations (release_id, id);

CREATE TABLE audit_events (
    id          bigserial PRIMARY KEY,
    at          timestamptz NOT NULL DEFAULT clock_timestamp(),
    actor       text        NOT NULL,
    action      text        NOT NULL,
    outcome     text        NOT NULL CHECK (outcome IN ('allowed', 'denied')),
    config      text        NOT NULL DEFAULT '',
    environment text        NOT NULL DEFAULT '',
    release_id  text        NOT NULL DEFAULT '',
    generation  bigint      NOT NULL DEFAULT 0,
    epoch       bigint      NOT NULL DEFAULT 0,
    reason      text        NOT NULL DEFAULT '',
    detail      jsonb       NOT NULL DEFAULT '{}'
);
CREATE INDEX audit_events_target ON audit_events (config, environment, id DESC);
CREATE INDEX audit_events_release ON audit_events (release_id, id DESC);

-- The audit log is append-only.
CREATE FUNCTION audit_events_append_only() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    RAISE EXCEPTION 'audit_events is append-only';
END $$;
CREATE TRIGGER audit_events_append_only BEFORE UPDATE OR DELETE ON audit_events
    FOR EACH ROW EXECUTE FUNCTION audit_events_append_only();
CREATE TRIGGER audit_events_no_truncate BEFORE TRUNCATE ON audit_events
    FOR EACH STATEMENT EXECUTE FUNCTION audit_events_append_only();

-- Results of commands that carried a request_id, for idempotent retries.
CREATE TABLE idempotency (
    subject    text        NOT NULL,
    request_id text        NOT NULL,
    method     text        NOT NULL,
    result_ref text        NOT NULL,
    created_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (subject, request_id)
);

-- Fencing token of the release controller (docs/invariants.md I4).
CREATE TABLE controller_leader (
    id          integer PRIMARY KEY CHECK (id = 1),
    epoch       bigint      NOT NULL,
    holder      text        NOT NULL,
    acquired_at timestamptz NOT NULL
);
INSERT INTO controller_leader (id, epoch, holder, acquired_at) VALUES (1, 0, '', now());
