import { DatabaseSync } from 'node:sqlite';
import { createHash, randomBytes, randomUUID, createCipheriv, createDecipheriv } from 'node:crypto';
export const hash = value => createHash('sha256').update(value).digest('hex');
export const token = () => 'hg_' + randomBytes(30).toString('base64url');
export const id = () => randomUUID();
export const now = () => Date.now();
export class AppError extends Error {
  constructor(status, message, code = 'invalid_request') { super(message); this.status = status; this.code = code; }
}
export function usageWindow(at=now()) {
  const day=86400000, offset=8*3600000;
  const today=Math.floor((at+offset)/day)*day-offset;
  return {start:today-6*day,end:today+day,timeZone:'Asia/Shanghai',days:7};
}
export class Store {
  constructor(filename,{encryptionKey}={}) {
    this.encryptionKey=encryptionKey;
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
    add('accounts','removed_at','INTEGER');
    add('members','removed_at','INTEGER');
    add('requests','tracking_only','INTEGER NOT NULL DEFAULT 0');
    const missingFlag=!this.all('PRAGMA table_info(requests)').some(c=>c.name==='usage_unknown');
    add('requests','usage_unknown','INTEGER NOT NULL DEFAULT 0');
    if(missingFlag)this.db.exec("UPDATE requests SET usage_unknown=1 WHERE status IN ('unknown','interrupted')");
    add('keys','secret_cipher','TEXT');
    this.db.exec('CREATE INDEX IF NOT EXISTS request_usage_time ON requests(COALESCE(ended,started))');
    add('requests','session_id','TEXT REFERENCES sessions(id)');add('requests','predicted','INTEGER NOT NULL DEFAULT 10000');
    add('sessions','cum_input','INTEGER NOT NULL DEFAULT 0');add('sessions','cum_output','INTEGER NOT NULL DEFAULT 0');add('sessions','cum_cached','INTEGER NOT NULL DEFAULT 0');
    add('sessions','protocol',"TEXT NOT NULL DEFAULT 'chat'");add('sessions','client_key','TEXT');
    this.db.exec('CREATE UNIQUE INDEX IF NOT EXISTS session_client ON sessions(member_id,protocol,client_key) WHERE client_key IS NOT NULL');
    const keyUsageExists=this.all('PRAGMA table_info(keys)').some(c=>c.name==='used');
    add('keys','unmetered','INTEGER NOT NULL DEFAULT 0');add('keys','quota','INTEGER');add('keys','used','INTEGER NOT NULL DEFAULT 0');add('keys','removed_at','INTEGER');
    if(!keyUsageExists)this.db.exec('UPDATE keys SET used=(SELECT COALESCE(SUM(input+output),0) FROM requests WHERE key_id=keys.id)');
    this.db.exec("BEGIN IMMEDIATE");
    try{
      this.db.exec("UPDATE keys SET unmetered=unmetered+(SELECT COUNT(*) FROM requests WHERE key_id=keys.id AND status='running' AND tracking_only=1)");
      this.db.exec("UPDATE sessions SET state='uncertain' WHERE id IN (SELECT session_id FROM requests WHERE status='running')");
      this.db.prepare("UPDATE requests SET status='interrupted',usage_unknown=1,ended=?,error=CASE WHEN tracking_only=1 THEN '服务重启；本次用量可能未计入累计' ELSE '服务重启；上游用量未知，保留预占额度待核对' END WHERE status='running'").run(now());
      this.db.prepare("UPDATE requests SET status='cancelled',reserved=0,ended=?,error='服务重启；排队请求未调用上游，已释放预占' WHERE status='queued'").run(now());
      this.db.exec("UPDATE accounts SET state='offline',last_error='服务重启中断了官方登录，请重新登录' WHERE state='login' AND removed_at IS NULL");
      this.db.exec('COMMIT');
    }catch(e){this.db.exec('ROLLBACK');throw e;}
    this.pruneUsage();
  }
  encryptKey(secret,keyId) {
    if(!this.encryptionKey)return null;
    const iv=randomBytes(12),cipher=createCipheriv('aes-256-gcm',this.encryptionKey,iv);
    cipher.setAAD(Buffer.from(keyId));
    const data=Buffer.concat([cipher.update(secret,'utf8'),cipher.final()]);
    return Buffer.concat([iv,cipher.getAuthTag(),data]).toString('base64');
  }
  revealKey(row) {
    if(!this.encryptionKey||!row.secret_cipher)return null;
    try {
      const data=Buffer.from(row.secret_cipher,'base64'),cipher=createDecipheriv('aes-256-gcm',this.encryptionKey,data.subarray(0,12));
      cipher.setAAD(Buffer.from(row.id));cipher.setAuthTag(data.subarray(12,28));
      const secret=Buffer.concat([cipher.update(data.subarray(28)),cipher.final()]).toString('utf8');
      return hash(secret)===row.digest?secret:null;
    }catch{return null;}
  }
  pruneUsage(at=now()) {
    const window=usageWindow(at);
    if(this.prunedStart!==window.start){
      this.run('UPDATE requests SET input=0,output=0,cached=0,usage_unknown=0 WHERE ended IS NOT NULL AND ended<? AND (input<>0 OR output<>0 OR cached<>0 OR usage_unknown<>0)',window.start);
      this.prunedStart=window.start;
    }
    return window;
  }
  usageFor(column,value,at=now()) {
    if(!['key_id','member_id','account_id'].includes(column))throw Error('Invalid usage scope');
    const w=this.pruneUsage(at);
    return this.get(`SELECT COALESCE(SUM(input+output),0) used,COALESCE(SUM(usage_unknown),0) unmetered FROM requests WHERE ${column}=? AND COALESCE(ended,started)>=? AND COALESCE(ended,started)<?`,value,w.start,w.end);
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
    if(!this.get('SELECT id FROM members WHERE id=? AND removed_at IS NULL',memberId)) throw new AppError(404,'员工不存在或已移除');
    const secret=token(), keyId=id();
    this.run('INSERT INTO keys(id,member_id,digest,prefix,expires,created,quota,secret_cipher) VALUES(?,?,?,?,?,?,?,?)',keyId,memberId,hash(secret),secret.slice(0,11),expires,now(),quota,this.encryptKey(secret,keyId));
    this.audit('key.create',keyId); return {keyId,secret};
  }
  authenticate(secret) {
    const row=this.get(`SELECT k.id key_id,k.secret_cipher,k.expires,k.enabled key_enabled,k.quota key_quota,k.used key_used,m.* FROM keys k JOIN members m ON m.id=k.member_id WHERE k.digest=? AND k.removed_at IS NULL AND m.removed_at IS NULL`,hash(secret));
    if(!row) throw new AppError(401,'Key 无效','invalid_api_key');
    if(!row.enabled || !row.key_enabled) throw new AppError(403,'Key 或员工已停用','key_disabled');
    if(row.expires!==null && row.expires<=now()) throw new AppError(403,'Key 已到期','key_expired');
    if(this.encryptionKey&&!row.secret_cipher)this.run('UPDATE keys SET secret_cipher=? WHERE id=? AND secret_cipher IS NULL',this.encryptKey(secret,row.key_id),row.key_id);
    delete row.secret_cipher;
    return row;
  }
  allowance(member,reserveSize) {
      const m=this.get('SELECT * FROM members WHERE id=?',member.id);
      const key=this.get('SELECT * FROM keys WHERE id=?',member.key_id);
      if(!m||m.removed_at!==null||!key||key.removed_at!==null||key.member_id!==m.id||!m.enabled||!key.enabled||(key.expires!==null&&key.expires<=now()))throw new AppError(403,'Key 或员工已停用或到期');
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
  observe(member,ip,agent,model){
    const requestId=id();
    this.run("INSERT INTO requests(id,member_id,key_id,started,status,ip,agent,reserved,model,predicted,tracking_only) VALUES(?,?,?,?,'queued',?,?,0,?,0,1)",requestId,member.id,member.key_id,now(),ip,agent,model);
    return requestId;
  }
  finish(requestId,result,after) {
    this.db.exec('BEGIN IMMEDIATE');
    try {
      const row=this.get('SELECT * FROM requests WHERE id=?',requestId);
      if(!row || !['queued','running'].includes(row.status)) throw Error('Request already settled');
      const {input=0,output=0,cached=0,status='ok',error=null,usageKnown=true}=result;
      this.run('UPDATE requests SET ended=?,status=?,input=?,output=?,cached=?,reserved=?,usage_unknown=?,error=? WHERE id=?',now(),status,input,output,cached,usageKnown?0:row.reserved,usageKnown?0:1,error,requestId);
      this.run('UPDATE members SET used=used+? WHERE id=?',input+output,row.member_id);
      this.run('UPDATE keys SET used=used+? WHERE id=?',input+output,row.key_id);
      if(row.tracking_only&&!usageKnown)this.run('UPDATE keys SET unmetered=unmetered+1 WHERE id=?',row.key_id);
      if(after)after();
      this.db.exec('COMMIT');
    } catch(e) {this.db.exec('ROLLBACK');throw e;}
  }
  snapshot(at=now()) {
    const window=this.pruneUsage(at),where='COALESCE(ended,started)>=? AND COALESCE(ended,started)<?',range=[window.start,window.end];
    const scope=(column,value)=>this.get(`SELECT COALESCE(SUM(input+output),0) used,COALESCE(SUM(usage_unknown),0) unmetered,COUNT(DISTINCT ip) ips,MAX(started) last_seen,COALESCE(SUM(reserved),0) reserved FROM requests WHERE ${column}=? AND ${where}`,value,...range);
    return {
      usageWindow:window,
      members:this.all('SELECT * FROM members WHERE removed_at IS NULL ORDER BY created DESC').map(m=>({...m,...scope('member_id',m.id)})),
      keys:this.all('SELECT * FROM keys WHERE removed_at IS NULL ORDER BY created DESC').map(k=>({id:k.id,member_id:k.member_id,prefix:k.prefix,full_key:this.revealKey(k),expires:k.expires,enabled:k.enabled,created:k.created,quota:k.quota,...scope('key_id',k.id)})),
      accounts:this.all('SELECT id,name,kind,enabled,state,last_used,last_error,quota_json,quota_error,token_budget FROM accounts WHERE removed_at IS NULL ORDER BY rowid').map(a=>({...a,budget_used:scope('account_id',a.id).used})),
      sessions:this.all('SELECT s.id,s.member_id,s.account_id,s.upstream_id,s.state,s.created,s.updated,s.protocol,m.name member_name,a.name account_name FROM sessions s JOIN members m ON m.id=s.member_id JOIN accounts a ON a.id=s.account_id WHERE s.updated>=? AND s.updated<? ORDER BY updated DESC LIMIT 100',...range).map(s=>({...s,request_count:this.get(`SELECT COUNT(*) n FROM requests WHERE session_id=? AND ${where}`,s.id,...range).n,upstream_thread:s.protocol==='responses'?s.id:s.upstream_id})),
      requests:this.all(`SELECT r.*,m.name member_name,a.name account_name,k.prefix key_prefix,k.removed_at key_removed_at FROM requests r LEFT JOIN members m ON m.id=r.member_id LEFT JOIN accounts a ON a.id=r.account_id LEFT JOIN keys k ON k.id=r.key_id WHERE ${where} ORDER BY started DESC LIMIT 200`,...range),
      totals:this.get(`SELECT COUNT(*) requests,COALESCE(SUM(input+output),0) tokens,COALESCE(SUM(CASE WHEN status='ok' THEN 1 ELSE 0 END),0) success,COALESCE(SUM(reserved),0) reserved,COALESCE(SUM(usage_unknown),0) unmetered FROM requests WHERE ${where}`,...range),
      daily:this.all(`SELECT strftime('%Y-%m-%d',COALESCE(ended,started)/1000,'unixepoch','+8 hours') day,SUM(input+output) tokens,COUNT(*) requests FROM requests WHERE ${where} GROUP BY day ORDER BY day`,...range),
      audit:this.all('SELECT * FROM audit WHERE at>=? AND at<? ORDER BY at DESC LIMIT 30',...range)
    };
  }
  close(){this.db.close();}
}
