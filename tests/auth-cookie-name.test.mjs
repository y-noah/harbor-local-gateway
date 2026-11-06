import test from 'node:test';
import assert from 'node:assert/strict';
import {mkdtemp,writeFile,readFile,rm,access} from 'node:fs/promises';
import {join} from 'node:path';
import {tmpdir} from 'node:os';
import {adminCookieName,createAdminAuth,passwordHash} from '../auth.mjs';
import {createGateway} from '../server.mjs';
const response=()=>({headers:{},setHeader(name,value){this.headers[name]=value;}}),password='synthetic-cookie-password';
test('cookie names default compatibly, accept bounded ASCII and reject unsafe explicit values',()=>{
 assert.equal(adminCookieName({}),'harbor_session');for(const name of ['A','harbor_session', 'harbor_session_preview','A-0_z','A'.repeat(64)])assert.equal(adminCookieName({adminCookieName:name}),name);
 for(const adminCookieName of ['',null,true,1,{},[], 'a'.repeat(65),'1a','_a',' a','a ','a\n','a\r\n','a;b','a=b','中文','a.b','a/b'])assert.throws(()=>createAdminAuth({adminCookieName}),{code:'admin_cookie_configuration'});
});
test('one fixed constructor name controls sync/async login, authentication and logout without legacy fallback',async()=>{
 const config={adminCookieName:'harbor_session_isolated',adminPasswordHash:passwordHash(password)},auth=createAdminAuth(config),legacy=createAdminAuth({adminPasswordHash:config.adminPasswordHash}),one=response(),two=response();auth.login(password,one);legacy.login(password,two);config.adminCookieName='untrusted_later_value';const again=response();await auth.loginAsync(password,again);assert.ok(again.headers['Set-Cookie'].startsWith('harbor_session_isolated='));
 const current=one.headers['Set-Cookie'].split(';')[0],old=two.headers['Set-Cookie'].split(';')[0],both=old+'; '+current;assert.equal(auth.authenticated({headers:{cookie:both,host:'evil.test',origin:'https://evil.test','x-forwarded-host':'other.test'}}),true);assert.equal(legacy.authenticated({headers:{cookie:both}}),true);assert.equal(auth.authenticated({headers:{cookie:old}}),false);assert.equal(auth.authenticated({headers:{cookie:current.replace('harbor_session_isolated=','harbor_session=')}}),false);
 const ended=response();auth.logout({headers:{cookie:both}},ended);assert.equal(ended.headers['Set-Cookie'],'harbor_session_isolated=; HttpOnly; SameSite=Strict; Path=/; Max-Age=0');assert.equal(auth.authenticated({headers:{cookie:both}}),false);assert.equal(legacy.authenticated({headers:{cookie:both}}),true);assert.equal(auth.authenticated({headers:{cookie:again.headers['Set-Cookie'].split(';')[0]}}),true);
 const rollback=createAdminAuth({...config,adminCookieName:'harbor_session'}),back=response();rollback.login(password,back);assert.ok(back.headers['Set-Cookie'].startsWith('harbor_session='));assert.equal(rollback.authenticated({headers:{cookie:current}}),false);
});
test('SQLite rejects invalid loaded configuration before generating password, key or database material',async()=>{
 const directory=await mkdtemp(join(tmpdir(),'harbor-cookie-invalid-')),path=join(directory,'settings.json'),config={port:43127,codexBinary:process.execPath,adminCookieName:'invalid;name'};try{const original=JSON.stringify(config);await writeFile(path,original,{mode:0o600});assert.throws(()=>createGateway({env:{},dataDir:directory,runtimeDir:join(directory,'runtime')}),{code:'admin_cookie_configuration'});assert.equal(await readFile(path,'utf8'),original);for(const file of ['管理密码.txt','key-encryption.key','gateway.sqlite'])await assert.rejects(access(join(directory,file)),{code:'ENOENT'});}finally{await rm(directory,{recursive:true,force:true});}
});
test('PG materials preflight shares the name validator before reading credential material',async()=>{
 const {certificateFixture}=await import('./device-fixture.mjs'),{postgresMaterialsPreflight}=await import('../ci/postgres-preflight.mjs'),directory=await mkdtemp(join(tmpdir(),'harbor-cookie-preflight-'));let certificates;
 try{certificates=await certificateFixture(directory);const env={HARBOR_DATA_DIR:directory,HARBOR_DEVICE_TLS_ENABLED:'1',HARBOR_DEVICE_TLS_PORT:'43128',HARBOR_DEVICE_PUBLIC_ORIGIN:'https://127.0.0.1:43128',HARBOR_PUBLIC_ORIGINS:'https://127.0.0.1:43128',HARBOR_DEVICE_TLS_KEY_FILE:join(directory,'server-key.pem'),HARBOR_DEVICE_TLS_CERT_FILE:join(directory,'server.pem'),HARBOR_DEVICE_CA_CERT_FILE:join(directory,'ca.pem'),HARBOR_DEVICE_CA_KEY_FILE:join(directory,'ca-key.pem')},base={adminPasswordHash:passwordHash(password)},options={env,database:{connection:{host:'/unused'}}};
 for(const patch of [{},{adminCookieName:'harbor_session_isolated'}]){await writeFile(join(directory,'settings.json'),JSON.stringify({...base,...patch}),{mode:0o600});await assert.rejects(postgresMaterialsPreflight(options),{code:'ENOENT'});}
 await writeFile(join(directory,'settings.json'),JSON.stringify({...base,adminCookieName:'unsafe\n'}),{mode:0o600});await assert.rejects(postgresMaterialsPreflight(options),{code:'admin_cookie_configuration'});
 }finally{certificates?.issuer.close();await rm(directory,{recursive:true,force:true});}
});
