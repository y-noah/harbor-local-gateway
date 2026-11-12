// Matches the deployed operator's restricted runtime boundary, without credentials.
export async function createUsageRollupRuntime(store){
 const {database}=await store.get('SELECT current_database() database'),role=database+'_runtime';
 if(!/^[a-z_][a-z0-9_]{0,62}$/.test(role))throw Error('synthetic_runtime_name_invalid');
 const quote=value=>'"'+value.replaceAll('"','""')+'"';
 await store.query('CREATE ROLE '+quote(role)+' NOLOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE NOINHERIT NOREPLICATION NOBYPASSRLS');
 await store.query('REVOKE ALL ON DATABASE '+quote(database)+' FROM PUBLIC');await store.query('GRANT CONNECT ON DATABASE '+quote(database)+' TO '+quote(role));
 await store.query('REVOKE ALL ON SCHEMA harbor,public FROM PUBLIC');await store.query('GRANT USAGE ON SCHEMA harbor TO '+quote(role));
 await store.query('GRANT SELECT,INSERT,UPDATE,DELETE ON ALL TABLES IN SCHEMA harbor TO '+quote(role));await store.query('GRANT USAGE,SELECT ON ALL SEQUENCES IN SCHEMA harbor TO '+quote(role));
 await store.query('CREATE SCHEMA harbor_operator');await store.query('REVOKE ALL ON SCHEMA harbor_operator FROM PUBLIC');await store.query('CREATE TABLE harbor_operator.activation(singleton boolean PRIMARY KEY CHECK(singleton),run_id text NOT NULL,binding text NOT NULL,database_oid text NOT NULL,runtime text NOT NULL,state text NOT NULL)');
 await store.run("INSERT INTO harbor_operator.activation SELECT true,'synthetic','synthetic',oid::text,$1,'active' FROM pg_database WHERE datname=current_database()",role);return role;
}
