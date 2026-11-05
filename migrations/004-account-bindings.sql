-- Account maintenance walks only that account's bindings in bounded pages.
CREATE INDEX sessions_account_bindings ON harbor.sessions(account_id,id);
