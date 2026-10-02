import { createGateway } from './server.mjs';
import { checkLogin } from './codex.mjs';
import { join } from 'node:path';
import { mkdirSync, existsSync } from 'node:fs';
import {readRateLimits,normalizeLimits} from './rates.mjs';
import {checkClaudeLogin} from './claude.mjs';
import {acquireInstance} from './instance.mjs';
import {fileURLToPath} from 'node:url';
const instance=await acquireInstance(fileURLToPath(new URL('./data/',import.meta.url))).catch(error=>{
  if(error.code==='EADDRINUSE'){console.log('Harbor is already running or initializing; initialization skipped.');process.exit(0);}
  throw error;
});
let app;
try{
app=createGateway();
if(!app.store.get('SELECT id FROM accounts LIMIT 1')){
  const home=join(app.runtime,'accounts','local');mkdirSync(home,{recursive:true});
  const ready=existsSync(join(home,'auth.json'))&&await checkLogin(app.config.codexBinary,home);
  app.store.run('INSERT INTO accounts(id,name,home,state) VALUES(?,?,?,?)','local','本机 Codex',home,ready?'ready':'offline');
}
for(const account of app.store.all('SELECT id,home,kind FROM accounts')){
  const ready=account.kind==='claude'?await checkClaudeLogin(app.config.claudeBinary||join(app.runtime,'claude-cli','node_modules','@anthropic-ai','claude-code','bin','claude.exe'),account.home):await checkLogin(app.config.codexBinary,account.home);
  app.store.run('UPDATE accounts SET state=?,last_error=NULL WHERE id=?',ready?'ready':'offline',account.id);
  if(ready&&account.kind==='codex'){try{const q=normalizeLimits(await readRateLimits(app.config.codexBinary,account.home));if(q)app.store.run('UPDATE accounts SET quota_json=?,quota_error=NULL,since_check_tokens=0 WHERE id=?',JSON.stringify(q),account.id);}catch(e){app.store.run('UPDATE accounts SET quota_error=? WHERE id=?',e.message,account.id);}}
}
console.log('Local workspace initialized.');
}finally{app?.store.close();await instance.release();}
