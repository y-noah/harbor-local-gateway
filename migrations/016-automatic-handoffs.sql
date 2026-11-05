CREATE TABLE harbor.automatic_handoff_groups(
 id TEXT PRIMARY KEY,plan_id TEXT NOT NULL REFERENCES harbor.routing_plans(id),slot TEXT NOT NULL,
 group_ordinal INTEGER NOT NULL CHECK(group_ordinal>0),requested BIGINT NOT NULL,expires BIGINT NOT NULL CHECK(expires>requested),
 status TEXT NOT NULL CHECK(status IN ('waiting','completed','cancelled','failed','superseded')),
 finished BIGINT,reason TEXT,CHECK((status='waiting')=(finished IS NULL)),UNIQUE(plan_id,group_ordinal)
);
CREATE INDEX automatic_handoff_waiting ON harbor.automatic_handoff_groups(requested,id) WHERE status='waiting';
ALTER TABLE harbor.session_handoffs ADD COLUMN automatic_group_id TEXT REFERENCES harbor.automatic_handoff_groups(id);
ALTER TABLE harbor.session_handoffs ADD COLUMN replaces_handoff_id TEXT REFERENCES harbor.session_handoffs(id);
CREATE INDEX handoff_automatic_group ON harbor.session_handoffs(automatic_group_id,id) WHERE automatic_group_id IS NOT NULL;
INSERT INTO harbor.gateway_settings(name,value) VALUES('request_handoff_policy','{"mode":"request-boundary","version":1}') ON CONFLICT(name) DO NOTHING;

-- Revisions preserve every immutable plan; the legacy row remains a slot summary.
CREATE TABLE harbor.nightly_plan_revisions(
 slot TEXT NOT NULL,revision INTEGER NOT NULL CHECK(revision>0),job_id TEXT NOT NULL UNIQUE,
 replaces_plan_id TEXT NOT NULL UNIQUE REFERENCES harbor.routing_plans(id),requested BIGINT NOT NULL,reason TEXT NOT NULL,
 PRIMARY KEY(slot,revision)
);
CREATE TABLE harbor.nightly_plan_applications(
 plan_id TEXT PRIMARY KEY REFERENCES harbor.routing_plans(id),slot TEXT NOT NULL,started BIGINT NOT NULL,ended BIGINT,
 status TEXT NOT NULL CHECK(status IN ('running','completed','failed','interrupted')),
 attempts INTEGER NOT NULL DEFAULT 0 CHECK(attempts>=0),summary TEXT
);
CREATE INDEX nightly_applications_slot ON harbor.nightly_plan_applications(slot,started,plan_id);
CREATE TABLE harbor.nightly_plan_group_results(
 plan_id TEXT NOT NULL REFERENCES harbor.nightly_plan_applications(plan_id),group_ordinal INTEGER NOT NULL CHECK(group_ordinal>0),
 at BIGINT NOT NULL,status TEXT NOT NULL CHECK(status IN ('applied','skipped')),reason TEXT,moved INTEGER NOT NULL CHECK(moved>=0),
 PRIMARY KEY(plan_id,group_ordinal)
);

-- Derive metadata from actual legacy checkpoints. Do not rewrite old plans,
-- batch summaries or audits, and never treat merely planned moves as applied.
INSERT INTO harbor.nightly_plan_applications(plan_id,slot,started,ended,status,attempts,summary)
 SELECT plan_id,day,started,ended,CASE WHEN status IN ('running','completed','failed','interrupted') THEN status ELSE 'interrupted' END,attempts,summary
 FROM harbor.balance_runs WHERE plan_id IS NOT NULL;
INSERT INTO harbor.nightly_plan_group_results(plan_id,group_ordinal,at,status,reason,moved)
 SELECT r.plan_id,g.group_ordinal,g.at,g.status,g.reason,g.moved
 FROM harbor.nightly_group_results g JOIN harbor.balance_runs r ON r.day=g.day WHERE r.plan_id IS NOT NULL;
