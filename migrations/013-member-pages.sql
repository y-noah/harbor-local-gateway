CREATE INDEX members_created_page ON harbor.members(created DESC,id DESC) WHERE removed_at IS NULL;
CREATE INDEX members_type_created_page ON harbor.members(usage_type_id,created DESC,id DESC) WHERE removed_at IS NULL;
CREATE INDEX keys_created_page ON harbor.keys(created DESC,id DESC) WHERE removed_at IS NULL;
CREATE INDEX keys_member_created_page ON harbor.keys(member_id,created DESC,id DESC) WHERE removed_at IS NULL;
CREATE INDEX request_member_usage ON harbor.requests(member_id,usage_at,id);
CREATE INDEX request_key_usage ON harbor.requests(key_id,usage_at,id);
