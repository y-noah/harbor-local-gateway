CREATE TABLE harbor.account_package_templates(
 id TEXT PRIMARY KEY,created BIGINT NOT NULL,latest_version BIGINT NOT NULL CHECK(latest_version>=1)
);
CREATE TABLE harbor.account_package_versions(
 template_id TEXT NOT NULL REFERENCES harbor.account_package_templates(id),version BIGINT NOT NULL CHECK(version>=1),
 created BIGINT NOT NULL,definition JSONB NOT NULL,PRIMARY KEY(template_id,version)
);
CREATE TABLE harbor.account_package_selections(
 account_id TEXT PRIMARY KEY REFERENCES harbor.accounts(id),mode TEXT NOT NULL CHECK(mode IN ('legacy','unknown','selected')),
 template_id TEXT,template_version BIGINT,revision BIGINT NOT NULL DEFAULT 0 CHECK(revision>=0),
 capacity_override JSONB,reason TEXT,official_confirmation JSONB,observed_plan TEXT,recognition_version BIGINT NOT NULL DEFAULT 0,
 updated BIGINT NOT NULL,
 FOREIGN KEY(template_id,template_version) REFERENCES harbor.account_package_versions(template_id,version),
 CHECK((mode='selected' AND template_id IS NOT NULL AND template_version IS NOT NULL) OR (mode<>'selected' AND template_id IS NULL AND template_version IS NULL))
);
CREATE INDEX account_package_selection_template ON harbor.account_package_selections(template_id,template_version,account_id);
INSERT INTO harbor.account_package_selections(account_id,mode,updated)
 SELECT id,'legacy',floor(extract(epoch FROM clock_timestamp())*1000)::bigint FROM harbor.accounts;
CREATE TABLE harbor.account_package_previews(
 id TEXT PRIMARY KEY,created BIGINT NOT NULL,expires BIGINT NOT NULL,template_id TEXT NOT NULL,expected_version BIGINT NOT NULL,
 definition JSONB NOT NULL,targets JSONB NOT NULL,overwrite_overrides JSONB NOT NULL,
 status TEXT NOT NULL DEFAULT 'preview' CHECK(status IN ('preview','applied')),result JSONB,finished BIGINT
);
CREATE INDEX account_package_preview_created ON harbor.account_package_previews(created DESC,id DESC);
INSERT INTO harbor.gateway_settings(name,value) VALUES('account_package_policy','{"enabled":false,"smoothing_hours":24,"version":0}') ON CONFLICT(name) DO NOTHING;
