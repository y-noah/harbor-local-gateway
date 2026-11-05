ALTER TABLE harbor.balance_runs ADD COLUMN plan_id TEXT REFERENCES harbor.routing_plans(id);
ALTER TABLE harbor.balance_runs ADD COLUMN attempts INTEGER NOT NULL DEFAULT 0 CHECK(attempts>=0);
CREATE UNIQUE INDEX balance_runs_plan ON harbor.balance_runs(plan_id) WHERE plan_id IS NOT NULL;
CREATE TABLE harbor.nightly_group_results(
 day TEXT NOT NULL REFERENCES harbor.balance_runs(day),group_ordinal INTEGER NOT NULL CHECK(group_ordinal>0),
 at BIGINT NOT NULL,status TEXT NOT NULL CHECK(status IN ('applied','skipped')),reason TEXT,
 moved INTEGER NOT NULL CHECK(moved>=0),PRIMARY KEY(day,group_ordinal)
);
CREATE INDEX jobs_nightly_ready ON harbor.background_jobs(target,status,created,id) WHERE kind='routing.nightly';
CREATE TABLE harbor.nightly_cycle_ready(day TEXT PRIMARY KEY,at BIGINT NOT NULL);
CREATE INDEX nightly_unfinished ON harbor.balance_runs(day) WHERE status='running' AND plan_id IS NOT NULL;
