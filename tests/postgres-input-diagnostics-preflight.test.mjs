import {verifyConnectionEnvironment} from '../ci/local-assurance-migrate.mjs';
import {createHash} from 'node:crypto';
import test from 'node:test';
import assert from 'node:assert/strict';
import {execFile} from 'node:child_process';
import {promisify} from 'node:util';
import {recordingInputDiagnosticsConfiguration} from '../recording-input-diagnostics.mjs';
import {postgresMaterialsPreflight} from '../ci/postgres-preflight.mjs';
const prefix='HARBOR_RECORDING_INPUT_DIAGNOSTICS';
test('宿主Python和Node对诊断缺省、合法边界及拒绝组合采用相同契约',async()=>{
 const cases=[{},...['0','1','01','true','',null].map(value=>({[prefix]:value}))];
 for(const [suffix,maximum] of [['MAX_REQUESTS',10000],['MAX_GROUPS',256],['MAX_ITEMS_PER_REQUEST',4096],['TTL_MS',3600000]])for(const value of ['1',String(maximum),'0',String(maximum+1),'-1','1.5',' 1','1 ','١','0'.repeat(100)+'1'])cases.push({[prefix]:'0',[prefix+'_'+suffix]:value});
 const expected=cases.map(env=>{try{return {accepted:true,value:recordingInputDiagnosticsConfiguration({env})};}catch{return {accepted:false};}});
 const script=`import importlib.util,json,sys
sys.dont_write_bytecode=True
s=importlib.util.spec_from_file_location('p','ci/postgres-deploy.py');p=importlib.util.module_from_spec(s);s.loader.exec_module(p)
results=[]
for values in json.loads(sys.argv[1]):
 try:
  v=p.input_diagnostics_configuration(values);out={'enabled':v[p.INPUT_DIAGNOSTICS_FLAG]=='1'}
  for suffix,key in [('MAX_REQUESTS','maxRequests'),('MAX_GROUPS','maxGroups'),('MAX_ITEMS_PER_REQUEST','maxItemsPerRequest'),('TTL_MS','ttlMs')]:out[key]=v[p.INPUT_DIAGNOSTICS_FLAG+'_'+suffix]
  results.append({'accepted':True,'value':out})
 except (ValueError,TypeError):results.append({'accepted':False})
print(json.dumps(results))`;
 const result=await promisify(execFile)('python3',['-c',script,JSON.stringify(cases)],{encoding:'utf8',timeout:10000,maxBuffer:65536});const actual=JSON.parse(result.stdout);assert.deepEqual(actual,expected);
 const base='HARBOR_DATABASE_URL=postgresql://synthetic@database.test/owned\nHARBOR_RECORDING_STORAGE_MODE=report\n',digest=createHash('sha256').update(base).digest('hex');
 const helperAccepted=cases.map(env=>{try{verifyConnectionEnvironment(base+Object.entries(env).map(([key,value])=>key+'='+value+'\n').join(''),digest);return true;}catch{return false;}});
 assert.deepEqual(helperAccepted,expected.map(value=>value.accepted),'standalone helper must accept precisely the same diagnostic values');

});
test('材料预检在读取设备、凭据或连接数据库之前拒绝非法诊断配置，即使开关关闭',async()=>{
 for(const env of [{[prefix]:'true'},{[prefix]:'0',[prefix+'_MAX_REQUESTS']:'0'},{[prefix]:'1',[prefix+'_TTL_MS']:'3600001'},{[prefix+'_MAX_GROUPS']:'1.5'}])await assert.rejects(postgresMaterialsPreflight({env}),error=>error.code==='recording_configuration'&&!error.message.includes('true'));
 for(const env of [{},{[prefix]:'0'},{[prefix]:'1',[prefix+'_MAX_GROUPS']:'1'}])await assert.rejects(postgresMaterialsPreflight({env}),error=>error.code!=='recording_configuration');
});
