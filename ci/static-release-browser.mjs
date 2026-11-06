// Real legacy SQLite HTTP + CSP + Chromium qualification, using only synthetic local data.
import assert from 'node:assert/strict';
import {mkdtemp,readFile,writeFile,rm,symlink} from 'node:fs/promises';
import {tmpdir} from 'node:os';
import {join,resolve} from 'node:path';
import {pathToFileURL} from 'node:url';
import {execFileSync} from 'node:child_process';
import {createHash} from 'node:crypto';
const path=resolve(process.argv[2]||''),bundle=JSON.parse(await readFile(path,'utf8'));
const {chromium}=await import(process.env.HARBOR_STATIC_PLAYWRIGHT_MODULE||'playwright');
const decode=value=>Buffer.from(value,'base64'),hash=value=>createHash('sha256').update(value).digest('hex');
const root=await mkdtemp(join(tmpdir(),'harbor-static-browser-'));let app,browser;
const cases=[],retained=[],secret='synthetic-static-browser-only';
const original=bundle.sources.find(row=>row.revision===bundle.backend_revision);assert.ok(original);
const oldFiles=Object.fromEntries(Object.entries(original.files).map(([k,v])=>[k,decode(v)]));
const compiled=Object.fromEntries(Object.entries(bundle.assets).map(([k,v])=>[k,decode(v)]));
const navigationFor=html=>[...html.toString().matchAll(/<button\b[^>]*\bdata-tab="([^"]+)"/g)].map(row=>row[1]);
const marker=(html,revision)=>revision===bundle.backend_revision?html:Buffer.from(html.toString().replace(/<html(?=\s|>)/i,`<html data-harbor-static-revision="${revision}"`));
const previousHtml=bundle.sources.map(row=>marker(decode(row.files['index.html']),row.revision)).find(html=>hash(html)===bundle.before['index.html']);assert.ok(previousHtml,'Previous index must be retained');
try{
 const archive=execFileSync('git',['archive','--format=tar',bundle.backend_revision,'--','*.mjs','migrations','public','clients','package.json','package-lock.json'],{maxBuffer:64*1024*1024});
 execFileSync('python3',['-c','import tarfile,sys,io; archive=tarfile.open(fileobj=io.BytesIO(sys.stdin.buffer.read())); members=archive.getmembers(); assert all(not m.name.startswith("/") and ".." not in m.name.split("/") and (m.isfile() or m.isdir()) for m in members); archive.extractall(sys.argv[1])',root],{input:archive});
 await symlink(resolve('node_modules'),join(root,'node_modules'),'dir');
 const {createGateway}=await import(pathToFileURL(join(root,'server.mjs')));
 const {passwordHash}=await import(pathToFileURL(join(root,'auth.mjs')));
 app=createGateway({dataDir:join(root,'synthetic-data'),runtimeDir:join(root,'synthetic-runtime'),config:{adminPasswordHash:passwordHash(secret),codexBinary:'unused',reserveTokens:100},env:{},pricingFetch:async()=>({ok:false,status:503}),rateReader:async()=>null,responsesTransport:{models:async()=>[]},checker:async()=>false});
 await new Promise(resolve=>app.server.listen(0,'127.0.0.1',resolve));const origin='http://127.0.0.1:'+app.server.address().port;
 browser=await chromium.launch({headless:true,...(process.env.HARBOR_STATIC_CHROMIUM?{executablePath:process.env.HARBOR_STATIC_CHROMIUM}:{})});
 const put=async(name,value)=>writeFile(join(root,'public',name),value);
 async function openPage(html,{cache=false,delay=false,whileDelayed}={}){
  await put('index.html',html);await put('app.js',compiled['app.js']);await put('style.css',oldFiles['style.css']);
  const context=await browser.newContext(),page=await context.newPage(),errors=[],apiFailures=[];
  assert.equal((await context.request.post(origin+'/api/auth/login',{data:{password:secret}})).status(),200);
  await context.addInitScript(()=>{window.staticCspViolations=[];document.addEventListener('securitypolicyviolation',e=>window.staticCspViolations.push(e.violatedDirective));});
  page.on('pageerror',e=>errors.push(e.message));
  page.on('response',r=>{if(r.url().startsWith(origin+'/api/admin/')&&r.status()>=400)apiFailures.push({path:new URL(r.url()).pathname,status:r.status()});});
  if(cache){await page.goto(origin);await page.locator('#shell:not(.hidden)').waitFor();}
  let release,entered,held;const requested=new Promise(resolve=>entered=resolve);
  if(delay){held=new Promise(resolve=>release=resolve);await page.route('**/app.js',async route=>{entered();await held;await route.continue();});}
  const loading=page.goto(origin,{waitUntil:'domcontentloaded'});
  try{
   if(delay){let timer;try{await Promise.race([requested,new Promise((_,reject)=>{timer=setTimeout(()=>reject(Error('delayed script request timeout')),10000);})]);}finally{clearTimeout(timer);}await whileDelayed();release();}
   await loading;await page.locator('#shell:not(.hidden)').waitFor({timeout:10000});
   await page.waitForFunction(()=>document.querySelector('#content')?.children.length>0&&document.adoptedStyleSheets.length>0);
   const buttons=await page.locator('nav [data-tab]').all();assert.ok(buttons.length>=5);
   for(const button of buttons){await button.click();await page.waitForFunction(()=>document.querySelector('#content')?.textContent.trim().length>0);}
   await page.waitForLoadState('networkidle',{timeout:10000});assert.deepEqual(apiFailures,[]);
   const result=await page.evaluate(()=>({violations:window.staticCspViolations,sheets:document.adoptedStyleSheets.length,stylesheetLinks:[...document.querySelectorAll('link[rel="stylesheet"]')].filter(x=>new URL(x.href).pathname==='/style.css').length,nav:[...document.querySelectorAll('nav [data-tab]')].map(x=>x.dataset.tab)}));
   assert.deepEqual(errors,[]);assert.deepEqual(result.violations,[]);assert.equal(result.sheets,1);assert.equal(result.stylesheetLinks,0);assert.deepEqual(result.nav,navigationFor(html));
   return {context,page,result,errors};
  }catch(e){release?.();const detail={cause:e.message,errors,violations:await page.evaluate(()=>window.staticCspViolations).catch(()=>[])};await context.close();throw new Error(JSON.stringify(detail));}
 }
 async function check(name,html,options){const row=await openPage(html,options);cases.push(name);return row;}
 let row=await check('old-html',oldFiles['index.html']);await row.context.close();
 row=await check('new-html',compiled['index.html']);await row.context.close();
 row=await check('old-delayed-script',oldFiles['index.html'],{delay:true,whileDelayed:()=>put('index.html',compiled['index.html'])});await row.context.close();
 row=await check('new-delayed-script',compiled['index.html'],{delay:true,whileDelayed:()=>put('index.html',oldFiles['index.html'])});await row.context.close();
 row=await check('rollback-live-new-page',compiled['index.html']);await put('index.html',previousHtml);await row.page.locator('nav [data-tab="overview"]').click();assert.deepEqual(await row.page.evaluate(()=>window.staticCspViolations),[]);await row.context.close();
 row=await check('rollback-old-html',previousHtml);await row.context.close();
 row=await check('cached-old-css',oldFiles['index.html'],{cache:true});await row.context.close();
 row=await check('cached-new-page',compiled['index.html'],{cache:true});await row.context.close();
 for(const source of bundle.sources){
  const html=marker(decode(source.files['index.html']),source.revision);row=await openPage(html);retained.push(source.revision);await row.context.close();
  row=await openPage(html,{delay:true,whileDelayed:()=>put('index.html',previousHtml)});await row.context.close();
 }
 assert.equal(hash(await readFile(join(root,'public','style.css'))),bundle.before['style.css']);
 const health=await fetch(origin+'/health'),denied=await fetch(origin+'/api/admin/state');assert.equal(health.status,200);assert.equal(denied.status,401);
 const rejected=await new Promise((resolve,reject)=>{import('node:http').then(({get})=>get(origin+'/health',{headers:{Host:'untrusted.invalid'}},r=>{r.resume();resolve(r.statusCode);}).on('error',reject));});assert.equal(rejected,403);
 for(const name of ['app.js','style.css']){const r=await fetch(origin+'/'+name,{headers:{'If-None-Match':'"synthetic-probe"'}});assert.equal(r.status,200);assert.equal(r.headers.get('cache-control'),'no-store');assert.equal(r.headers.has('etag'),false);}
 bundle.compatibility={contract:'harbor-static-bootstrap-v1',backend_revision:bundle.backend_revision,revision:bundle.revision,before:bundle.before,after:bundle.after,source_hashes:bundle.source_hashes,cases,passed:true};
 await writeFile(path,JSON.stringify(bundle));
 console.log(JSON.stringify({passed:true,cases,retained,backendRevision:bundle.backend_revision,onlineConnected:false,cspUnchanged:true,legacyRealHttp:true,allResourcesClosedInFinally:true}));
}finally{
 await browser?.close();await app?.close();await rm(root,{recursive:true,force:true});
}
