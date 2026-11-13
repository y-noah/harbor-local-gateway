-- Keep existing account-specific facts; only previews and policy need new scope.
ALTER TABLE harbor.extra_usage_previews ADD COLUMN department_id TEXT;
ALTER TABLE harbor.extra_usage_previews ADD COLUMN actor_kind TEXT;
ALTER TABLE harbor.extra_usage_previews ADD COLUMN actor_id TEXT;
ALTER TABLE harbor.extra_usage_previews ADD COLUMN actor_version BIGINT;
ALTER TABLE harbor.extra_usage_preview_items ADD COLUMN department_id TEXT;
UPDATE harbor.extra_usage_previews SET department_id='department-default',actor_kind='legacy_admin',actor_id='migration-v1',actor_version=0;
UPDATE harbor.extra_usage_preview_items i SET department_id=a.department_id FROM harbor.accounts a WHERE a.id=i.account_id;
ALTER TABLE harbor.extra_usage_previews ALTER COLUMN department_id SET NOT NULL;
ALTER TABLE harbor.extra_usage_previews ALTER COLUMN actor_kind SET NOT NULL;
ALTER TABLE harbor.extra_usage_previews ALTER COLUMN actor_id SET NOT NULL;
ALTER TABLE harbor.extra_usage_previews ALTER COLUMN actor_version SET NOT NULL;
ALTER TABLE harbor.extra_usage_preview_items ALTER COLUMN department_id SET NOT NULL;
ALTER TABLE harbor.extra_usage_previews ADD CONSTRAINT department_extra_preview_fk FOREIGN KEY(department_id) REFERENCES harbor.departments(id);
ALTER TABLE harbor.extra_usage_previews ADD CONSTRAINT department_extra_preview_identity UNIQUE(department_id,id);
ALTER TABLE harbor.extra_usage_previews ADD CONSTRAINT department_extra_preview_parent_fk FOREIGN KEY(department_id,parent_id) REFERENCES harbor.extra_usage_previews(department_id,id);
ALTER TABLE harbor.extra_usage_previews ADD CONSTRAINT department_extra_preview_actor CHECK(actor_kind IN ('super_admin','department_admin','legacy_admin') AND length(actor_id)>0 AND actor_version>=0);
ALTER TABLE harbor.extra_usage_preview_items ADD CONSTRAINT department_extra_preview_item_fk FOREIGN KEY(department_id,preview_id) REFERENCES harbor.extra_usage_previews(department_id,id);
ALTER TABLE harbor.extra_usage_preview_items ADD CONSTRAINT department_extra_item_account_fk FOREIGN KEY(department_id,account_id) REFERENCES harbor.accounts(department_id,id);
CREATE INDEX department_extra_previews_page ON harbor.extra_usage_previews(department_id,created DESC,id DESC);
CREATE TRIGGER department_identity_immutable BEFORE UPDATE OF department_id ON harbor.extra_usage_previews FOR EACH ROW EXECUTE FUNCTION harbor.department_identity_immutable();
CREATE TRIGGER department_identity_immutable BEFORE UPDATE OF department_id ON harbor.extra_usage_preview_items FOR EACH ROW EXECUTE FUNCTION harbor.department_identity_immutable();
CREATE TRIGGER department_batch_actor_immutable BEFORE UPDATE OF actor_kind,actor_id,actor_version ON harbor.extra_usage_previews FOR EACH ROW EXECUTE FUNCTION harbor.department_batch_actor_immutable();
INSERT INTO harbor.department_settings(department_id,name,value,version)
 SELECT 'department-default','extra_usage_policy',jsonb_build_object('enabled',enabled,'version',version),version FROM harbor.extra_usage_policy WHERE singleton=1;
