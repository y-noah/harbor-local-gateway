CREATE TABLE harbor.department_admin_key_secrets (
 department_id TEXT NOT NULL,
 key_id TEXT NOT NULL,
 secret_cipher TEXT NOT NULL CHECK(length(secret_cipher) BETWEEN 40 AND 2048),
 secret_digest TEXT NOT NULL CHECK(secret_digest ~ '^[a-f0-9]{64}$'),
 PRIMARY KEY (department_id,key_id),
 FOREIGN KEY (department_id,key_id) REFERENCES harbor.department_admin_keys(department_id,id) ON DELETE CASCADE
);
REVOKE ALL ON harbor.department_admin_key_secrets FROM PUBLIC;
