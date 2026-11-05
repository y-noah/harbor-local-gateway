CREATE TABLE harbor.member_types(
 id TEXT PRIMARY KEY,code TEXT NOT NULL UNIQUE,name TEXT NOT NULL,description TEXT NOT NULL,
 enabled INTEGER NOT NULL DEFAULT 1 CHECK(enabled IN (0,1)),version BIGINT NOT NULL DEFAULT 0 CHECK(version>=0),
 default_version BIGINT,updated BIGINT NOT NULL DEFAULT 0
);
CREATE TABLE harbor.member_type_templates(
 type_id TEXT NOT NULL REFERENCES harbor.member_types(id),version BIGINT NOT NULL CHECK(version>0),created BIGINT NOT NULL,
 allowed_models JSONB NOT NULL,usage_limits JSONB NOT NULL,
 PRIMARY KEY(type_id,version),CHECK(jsonb_typeof(allowed_models) IN ('null','array')),CHECK(jsonb_typeof(usage_limits)='object')
);
ALTER TABLE harbor.member_types ADD FOREIGN KEY(id,default_version) REFERENCES harbor.member_type_templates(type_id,version);
INSERT INTO harbor.member_types(id,code,name,description) VALUES
 ('usage-a','A','复杂研发','适合处理复杂研发任务，开展技术方案设计和疑难问题排查。'),
 ('usage-b','B','日常开发','适合完成日常功能开发、问题修复和测试工作。'),
 ('usage-c','C','按方案开发','适合按照明确方案完成开发任务，并处理常规调试问题。'),
 ('usage-d','D','开发辅助','适合辅助理解代码、完成简单修改，以及开展测试和学习。'),
 ('usage-e','E','方案与原型','适合编写方案、准备演示，并制作原型图或原型页面。'),
 ('usage-f','F','文档处理','适合整理、总结和改写文档，以及提取所需信息。');
ALTER TABLE harbor.members ADD COLUMN usage_type_id TEXT REFERENCES harbor.member_types(id);
ALTER TABLE harbor.members ADD COLUMN usage_type_version BIGINT NOT NULL DEFAULT 0 CHECK(usage_type_version>=0);
ALTER TABLE harbor.members ADD COLUMN default_models TEXT;
ALTER TABLE harbor.members ADD COLUMN default_models_set INTEGER NOT NULL DEFAULT 0 CHECK(default_models_set IN (0,1));
ALTER TABLE harbor.members ADD COLUMN defaults_version BIGINT NOT NULL DEFAULT 0 CHECK(defaults_version>=0);
ALTER TABLE harbor.members ADD COLUMN configuration_source JSONB NOT NULL DEFAULT '{"models":{"kind":"legacy"},"limits":{"kind":"legacy"}}';
CREATE INDEX members_type ON harbor.members(usage_type_id,id) WHERE removed_at IS NULL;
ALTER TABLE harbor.keys ADD COLUMN model_source JSONB NOT NULL DEFAULT '{"kind":"legacy"}';
ALTER TABLE harbor.requests ADD COLUMN usage_type_id TEXT REFERENCES harbor.member_types(id);
ALTER TABLE harbor.requests ADD COLUMN usage_type_code TEXT;
ALTER TABLE harbor.requests ADD COLUMN usage_type_name TEXT;
ALTER TABLE harbor.requests ADD COLUMN usage_type_version BIGINT;
CREATE INDEX request_type_page ON harbor.requests(usage_type_id,usage_at DESC,id DESC);
