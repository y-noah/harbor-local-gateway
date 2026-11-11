import test from 'node:test';
import assert from 'node:assert/strict';
import {join} from 'node:path';
import {readFile} from 'node:fs/promises';
import {createPrivatePostgres} from '../tools/capacity/owned-runtime.mjs';
import {PostgresStore} from '../postgres-store.mjs';
import {createPostgresGateway} from '../postgres-gateway.mjs';
import {passwordHash} from '../auth.mjs';
const binDirectory=process.env.HARBOR_CAPACITY_PG_BIN,libraryDirectory=process.env.HARBOR_CAPACITY_PG_LIB;

test('配置公网Origin后管理端API基址仍含v1，图片MCP下载与图片路由可用且鉴权保持',{skip:!binDirectory?'requires private PG18':false,timeout:30000},async()=>{
 const lab=await createPrivatePostgres({binDirectory,libraryDirectory});let app;
 try{
  const setup=new PostgresStore({connection:lab.connection});try{await setup.migrate();}finally{await setup.close();}
  const external='https://gateway.example.test:8443';
  app=await createPostgresGateway({env:{HARBOR_PUBLIC_ORIGINS:external},database:{connection:lab.connection},dataDir:join(lab.directory,'endpoint-app'),runtimeDir:join(lab.directory,'endpoint-runtime'),config:{port:43127,codexBinary:process.execPath,adminPasswordHash:passwordHash('synthetic-endpoint-admin')},recording:{enabled:false}});await new Promise(resolve=>app.server.listen(0,'127.0.0.1',resolve));const origin='http://127.0.0.1:'+app.server.address().port;
  const login=await fetch(origin+'/api/auth/login',{method:'POST',headers:{'content-type':'application/json'},body:JSON.stringify({password:'synthetic-endpoint-admin'})});assert.equal(login.status,200);const cookie=login.headers.get('set-cookie').split(';')[0];
  const state=await(await fetch(origin+'/api/admin/state?people=none&view=overview',{headers:{cookie}})).json();assert.equal(state.endpoint,external+'/v1');const scriptPath=new URL(state.endpoint+'/codex/image-mcp.mjs').pathname;assert.equal(scriptPath,'/v1/codex/image-mcp.mjs');
  assert.equal((await fetch(origin+scriptPath)).status,401);assert.equal((await fetch(origin+scriptPath,{headers:{cookie}})).status,401);
  const created=await fetch(origin+'/api/admin/members',{method:'POST',headers:{cookie,'content-type':'application/json'},body:JSON.stringify({name:'合成图片接入员工',quota:null,expires:null})});assert.equal(created.status,201);const member=await created.json(),headers={authorization:'Bearer '+member.secret};
  const script=await fetch(origin+scriptPath,{headers});assert.equal(script.status,200);assert.match(script.headers.get('content-type'),/^text\/javascript/);assert.equal(await script.text(),await readFile(new URL('../clients/image-mcp.mjs',import.meta.url),'utf8'));assert.equal((await app.store.get('SELECT COUNT(*) n FROM harbor.requests')).n,0);
  for(const suffix of ['/images/generations','/images/edits']){const path=new URL(state.endpoint+suffix).pathname;const response=await fetch(origin+path,{method:'POST',headers:{...headers,'content-type':'application/json'},body:'{}'});assert.equal(response.status,400);assert.notEqual((await response.json()).error.code,'not_found');}
 }finally{await app?.close();await lab.close();}
});
