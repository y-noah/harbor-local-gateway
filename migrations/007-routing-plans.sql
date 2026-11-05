CREATE TABLE harbor.routing_plans(
 id TEXT PRIMARY KEY REFERENCES harbor.background_jobs(id),
 created BIGINT NOT NULL,expires BIGINT NOT NULL CHECK(expires>created),
 header JSONB NOT NULL,bytes INTEGER NOT NULL CHECK(bytes>0)
);
CREATE INDEX routing_plans_expiry ON harbor.routing_plans(expires,id);
CREATE TABLE harbor.routing_plan_items(
 plan_id TEXT NOT NULL REFERENCES harbor.routing_plans(id) ON DELETE CASCADE,
 section TEXT NOT NULL CHECK(section IN ('moves','skipped','groups','fixed','accounts','sessions')),
 ordinal INTEGER NOT NULL CHECK(ordinal>0),payload JSONB NOT NULL,
 bytes INTEGER NOT NULL CHECK(bytes>0),PRIMARY KEY(plan_id,section,ordinal)
);
