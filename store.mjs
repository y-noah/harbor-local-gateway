import { DatabaseSync } from 'node:sqlite';
import { createHash, randomBytes, randomUUID } from 'node:crypto';
export const hash = value => createHash('sha256').update(value).digest('hex');
export const token = () => 'hg_' + randomBytes(30).toString('base64url');
export const id = () => randomUUID();
export const now = () => Date.now();
export class AppError extends Error {
  constructor(status, message, code = 'invalid_request') { super(message); this.status = status; this.code = code; }
}
export class Store {
  constructor(filename) {
    this.db = new DatabaseSync(filename);
    this.db.exec(`PRAGMA journal_mode=WAL; PRAGMA foreign_keys=ON; PRAGMA busy_timeout=5000;
      CREATE TABLE IF NOT EXISTS members(id TEXT PRIMARY KEY,name TEXT NOT NULL,quota INTEGER,used INTEGER NOT NULL DEFAULT 0,enabled INTEGER NOT NULL DEFAULT 1,created INTEGER NOT NULL);
      CREATE TABLE IF NOT EXISTS keys(id TEXT PRIMARY KEY,member_id TEXT NOT NULL REFERENCES members(id),digest TEXT UNIQUE NOT NULL,prefix TEXT NOT NULL,expires INTEGER,enabled INTEGER NOT NULL DEFAULT 1,created INTEGER NOT NULL);
      CREATE TABLE IF NOT EXISTS accounts(id TEXT PRIMARY KEY,name TEXT NOT NULL,kind TEXT NOT NULL DEFAULT 'codex',home TEXT NOT NULL,enabled INTEGER NOT NULL DEFAULT 1,state TEXT NOT NULL DEFAULT 'unknown',last_used INTEGER NOT NULL DEFAULT 0,last_error TEXT);
      CREATE TABLE IF NOT EXISTS requests(id TEXT PRIMARY KEY,member_id TEXT REFERENCES members(id),key_id TEXT REFERENCES keys(id),account_id TEXT REFERENCES accounts(id),started INTEGER NOT NULL,ended INTEGER,status TEXT NOT NULL,ip TEXT NOT NULL,agent TEXT,model TEXT,input INTEGER NOT NULL DEFAULT 0,output INTEGER NOT NULL DEFAULT 0,cached INTEGER NOT NULL DEFAULT 0,reserved INTEGER NOT NULL DEFAULT 0,error TEXT);
      CREATE INDEX IF NOT EXISTS request_member ON requests(member_id,started);
      CREATE INDEX IF NOT EXISTS request_time ON requests(started);
      CREATE TABLE IF NOT EXISTS audit(id TEXT PRIMARY KEY,at INTEGER NOT NULL,action TEXT NOT NULL,detail TEXT NOT NULL);
      CREATE TABLE IF NOT EXISTS sessions(id TEXT PRIMARY KEY,member_id TEXT NOT NULL REFERENCES members(id),account_id TEXT NOT NULL REFERENCES accounts(id),upstream_id TEXT,state TEXT NOT NULL DEFAULT 'new',message_count INTEGER NOT NULL DEFAULT 0,history_digest TEXT NOT NULL DEFAULT '',created INTEGER NOT NULL,updated INTEGER NOT NULL);
    `);
    const add=(table,column,definition)=>{if(!this.all('PRAGMA table_info('+table+')').some(c=>c.name===column))this.db.exec(`ALTER TABLE ${table} ADD COLUMN ${column} ${definition}`);};
    add('accounts','quota_json','TEXT');add('accounts','quota_error','TEXT');add('accounts','token_budget','INTEGER');add('accounts','budget_used','INTEGER NOT NULL DEFAULT 0');add('accounts','since_check_tokens','INTEGER NOT NULL DEFAULT 0');add('accounts','tokens_per_percent','REAL NOT NULL DEFAULT 10000');add('accounts','quota_calibration','TEXT');
    add('requests','session_id','TEXT REFERENCES sessions(id)');add('requests','predicted','INTEGER NOT NULL DEFAULT 10000');
    add('sessions','cum_input','INTEGER NOT NULL DEFAULT 0');add('sessions','cum_output','INTEGER NOT NULL DEFAULT 0');add('sessions','cum_cached','INTEGER NOT NULL DEFAULT 0');
    const keyUsageExists=this.all('PRAGMA table_info(keys)').some(c=>c.name==='used');
    add('keys','quota','INTEGER');add('keys','used','INTEGER NOT NULL DEFAULT 0');
    if(!keyUsageExists)this.db.exec('UPDATE keys SET used=(SELECT COALESCE(SUM(input+output),0) FROM requests WHERE key_id=keys.id)');
    this.db.exec("BEGIN IMMEDIATE");
    try{
      this.db.exec("UPDATE sessions SET state='uncertain' WHERE id IN (SELECT session_id FROM requests WHERE status='running')");
      this.db.prepare("UPDATE requests SET status='interrupted',ended=?,error='服务重启；上游用量未知，保留预占额度待核对' WHERE status='running'").run(now());
      this.db.prepare("UPDATE requests SET status='cancelled',reserved=0,ended=?,error='服务重启；排队请求未调用上游，已释放预占' WHERE status='queued'").run(now());
      this.db.exec('COMMIT');
    }catch(e){this.db.exec('ROLLBACK');throw e;}
  }
  all(sql,...args) { return this.db.prepare(sql).all(...args); }
  get(sql,...args) { return this.db.prepare(sql).get(...args); }
  run(sql,...args) { return this.db.prepare(sql).run(...args); }
  audit(action,detail) { this.run('INSERT INTO audit VALUES(?,?,?,?)',id(),now(),action,detail); }
  createMember(name,quota,expires) {
    const memberId=id(); this.run('INSERT INTO members(id,name,quota,created) VALUES(?,?,?,?)',memberId,name,quota,now());
    const key=this.createKey(memberId,expires); this.audit('member.create',name); return {memberId,...key};
  }
  createKey(memberId,expires=null,quota=null) {
    if(!this.get('SELECT id FROM members WHERE id=?',memberId)) throw new AppError(404,'员工不存在');
    const secret=token(), keyId=id();
    this.run('INSERT INTO keys(id,member_id,digest,prefix,expires,created,quota) VALUES(?,?,?,?,?,?,?)',keyId,memberId,hash(secret),secret.slice(0,11),expires,now(),quota);
    this.audit('key.create',keyId); return {keyId,secret};
  }
  authenticate(secret) {
    const row=this.get(`SELECT k.id key_id,k.expires,k.enabled key_enabled,k.quota key_quota,k.used key_used,m.* FROM keys k JOIN members m ON m.id=k.member_id WHERE k.digest=?`,hash(secret));
    if(!row) throw new AppError(401,'Key 无效','invalid_api_key');
    if(!row.enabled || !row.key_enabled) throw new AppError(403,'Key 或员工已停用','key_disabled');
    if(row.expires!==null && row.expires<=now()) throw new AppError(403,'Key 已到期','key_expired');
    return row;
  }
  allowance(member,reserveSize) {
      const m=this.get('SELECT * FROM members WHERE id=?',member.id);
      const key=this.get('SELECT * FROM keys WHERE id=?',member.key_id);
      if(!m||!key||key.member_id!==m.id||!m.enabled||!key.enabled||(key.expires!==null&&key.expires<=now()))throw new AppError(403,'Key 或员工已停用或到期');
      const pending=this.get("SELECT COALESCE(SUM(reserved),0) n FROM requests WHERE member_id=? AND status IN ('queued','running','interrupted','unknown')",m.id).n;
      const keyPending=this.get("SELECT COALESCE(SUM(reserved),0) n FROM requests WHERE key_id=? AND status IN ('queued','running','interrupted','unknown')",key.id).n;
      if(this.get("SELECT id FROM requests WHERE member_id=? AND status IN ('queued','running')",m.id)) throw new AppError(429,'该员工已有请求在处理，请稍后再试','member_busy');
      if(m.quota!==null && m.used+pending>=m.quota) throw new AppError(429,'额度已用完或被待核对请求占用','quota_exceeded');
      if(key.quota!==null&&key.used+keyPending>=key.quota)throw new AppError(429,'此 Key 的独立额度已用完或被预占','key_quota_exceeded');
      return Math.min(reserveSize,m.quota===null?Infinity:m.quota-m.used-pending,key.quota===null?Infinity:key.quota-key.used-keyPending);
  }
  reserve(member,ip,agent,reserveSize,model) {
    this.db.exec('BEGIN IMMEDIATE');
    try {
      const reserved=this.allowance(member,reserveSize);
      const requestId=id();
      this.run("INSERT INTO requests(id,member_id,key_id,started,status,ip,agent,reserved,model) VALUES(?,?,?,?,'queued',?,?,?,?)",requestId,member.id,member.key_id,now(),ip,agent,reserved,model);
      this.db.exec('COMMIT'); return requestId;
    } catch(e) { this.db.exec('ROLLBACK'); throw e; }
  }
  finish(requestId,result,after) {
    this.db.exec('BEGIN IMMEDIATE');
    try {
      const row=this.get('SELECT * FROM requests WHERE id=?',requestId);
      if(!row || !['queued','running'].includes(row.status)) throw Error('Request already settled');
      const {input=0,output=0,cached=0,status='ok',error=null,usageKnown=true}=result;
      this.run('UPDATE requests SET ended=?,status=?,input=?,output=?,cached=?,reserved=?,error=? WHERE id=?',now(),status,input,output,cached,usageKnown?0:row.reserved,error,requestId);
      this.run('UPDATE members SET used=used+? WHERE id=?',input+output,row.member_id);
      this.run('UPDATE keys SET used=used+? WHERE id=?',input+output,row.key_id);
      if(after)after();
      this.db.exec('COMMIT');
    } catch(e) {this.db.exec('ROLLBACK');throw e;}
  }
  snapshot() {
    return {
      members:this.all(`SELECT m.*,(SELECT COUNT(DISTINCT ip) FROM requests r WHERE r.member_id=m.id) ips,(SELECT MAX(started) FROM requests r WHERE r.member_id=m.id) last_seen,(SELECT COALESCE(SUM(reserved),0) FROM requests r WHERE r.member_id=m.id) reserved FROM members m ORDER BY created DESC`),
      keys:this.all('SELECT k.id,k.member_id,k.prefix,k.expires,k.enabled,k.created,k.quota,k.used,(SELECT COUNT(DISTINCT ip) FROM requests r WHERE r.key_id=k.id) ips FROM keys k ORDER BY created DESC'),
      accounts:this.all('SELECT id,name,kind,enabled,state,last_used,last_error,quota_json,quota_error,token_budget,budget_used,since_check_tokens,tokens_per_percent FROM accounts ORDER BY rowid'),
      sessions:this.all('SELECT s.*,m.name member_name,a.name account_name FROM sessions s JOIN members m ON m.id=s.member_id JOIN accounts a ON a.id=s.account_id ORDER BY updated DESC LIMIT 100'),
      requests:this.all('SELECT r.*,m.name member_name,a.name account_name FROM requests r LEFT JOIN members m ON m.id=r.member_id LEFT JOIN accounts a ON a.id=r.account_id ORDER BY started DESC LIMIT 200'),
      totals:this.get("SELECT COUNT(*) requests,COALESCE(SUM(input+output),0) tokens,COALESCE(SUM(CASE WHEN status='ok' THEN 1 ELSE 0 END),0) success,COALESCE(SUM(reserved),0) reserved FROM requests"),
      daily:this.all("SELECT strftime('%Y-%m-%d',started/1000,'unixepoch','+8 hours') day,SUM(input+output) tokens,COUNT(*) requests FROM requests WHERE started>? GROUP BY day ORDER BY day",now()-7*86400000),
      audit:this.all('SELECT * FROM audit ORDER BY at DESC LIMIT 30')
    };
  }
  close(){this.db.close();}
}
