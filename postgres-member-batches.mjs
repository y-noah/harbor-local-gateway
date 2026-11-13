import {createMemberBatchDepartments} from './postgres-member-batch-departments.mjs';
import {createPostgresAudit} from './postgres-audit.mjs';
import {isDeepStrictEqual as equal} from 'node:util';
import {AppError,id,storedModelAllowlist,usageWindow} from './store.mjs';
import {createPostgresJobs} from './postgres-jobs.mjs';
import {createPostgresMemberLimits,postgresMemberLimitConfiguration} from './postgres-member-limits.mjs';

const fail=(status,message,code='member_batch_invalid')=>{throw new AppError(status,message,code);};
const uuid=value=>typeof value==='string'&&/^[a-f0-9-]{36}$/.test(value);
const fields=(value,allowed)=>value&&typeof value==='object'&&!Array.isArray(value)&&Object.keys(value).every(key=>allowed.includes(key));
const inherited=source=>['type','employee'].includes(source?.kind)&&typeof source.type_id==='string';

// A preview freezes non-secret configuration and the exact Key set. Each member
// effect and its result share one lease-checked transaction; counters never move.
export function createPostgresMemberBatches(store,{readStore=store,clock=Date.now,departmentActors,jobs=createPostgresJobs(store,{clock,departmentActors,scopedKinds:['member.config']}),queryQueue,maxTargets=1000,maxKeysPerMember=1000,maxPreviewBytes=4194304,pageBytes=262144,ttlMs=3600000}={}){
  if(!Number.isInteger(maxTargets)||maxTargets<1||maxTargets>10000||!Number.isInteger(maxKeysPerMember)||maxKeysPerMember<1||maxKeysPerMember>10000||!Number.isInteger(maxPreviewBytes)||maxPreviewBytes<1024||maxPreviewBytes>67108864||!Number.isInteger(pageBytes)||pageBytes<1024||pageBytes>2097152||!Number.isInteger(ttlMs)||ttlMs<60000||ttlMs>86400000)fail(503,'批量配置资源设置无效','member_batch_configuration');
  const departments=createMemberBatchDepartments(store,jobs,'member_config_batches');
  const limits=createPostgresMemberLimits(store,{clock}),read=(op,{signal}={})=>queryQueue?queryQueue.run(op,{signal}):op();
  async function snapshot(target,memberId,{lock=false,departmentScope=null}={}){
    const member=await target.get('SELECT id,name,usage_type_id,usage_type_version,default_models,default_models_set,defaults_version,configuration_source,usage_limits,usage_limits_version FROM harbor.members WHERE id=$1 AND removed_at IS NULL'+(departmentScope?.view==='department'?' AND department_id=$2':'')+(lock?' FOR UPDATE':''),memberId,...(departmentScope?.view==='department'?[departmentScope.department_id]:[]));
    if(!member)return null;
    const keys=await target.all('SELECT id,allowed_models,model_policy_version,model_source FROM harbor.keys WHERE member_id=$1 AND removed_at IS NULL ORDER BY id LIMIT $2'+(lock?' FOR UPDATE':''),memberId,maxKeysPerMember+1);
    if(keys.length>maxKeysPerMember)fail(413,'员工Key数量超过完整配置预览容量','member_batch_capacity');
    const {name,...configuration}=member,usageType=member.usage_type_id?await target.get('SELECT code,name FROM harbor.member_types WHERE id=$1',member.usage_type_id):null;
    return {name,configuration:{...configuration,usage_type_label:usageType?usageType.code+' · '+usageType.name:'未分类',default_models:storedModelAllowlist(member.default_models),usage_limits:postgresMemberLimitConfiguration(member),keys:keys.map(key=>({...key,allowed_models:storedModelAllowlist(key.allowed_models)}))}};
  }
  async function type(target,typeId,departmentScope=null){
    const value=await target.get('SELECT t.id,t.code,t.name,t.description,t.enabled,t.version,t.default_version,p.allowed_models,p.usage_limits FROM harbor.member_types t LEFT JOIN harbor.member_type_templates p ON p.type_id=t.id AND p.version=t.default_version WHERE t.id=$1'+(departmentScope?.view==='department'?' AND t.department_id=$2':''),typeId,...(departmentScope?.view==='department'?[departmentScope.department_id]:[]));
    if(!value||!value.enabled)fail(400,'目标使用类型不存在或已停用');return value;
  }
  async function preview(payload,{scope:trustedScope}={}){
    const authorization=await departments.resolve(trustedScope,{write:true});
    if(!fields(payload,['scope','type_id','expected_type_version','models','limits','classify','overwrite_exceptions'])||typeof payload.type_id!=='string'||!Number.isSafeInteger(payload.expected_type_version)||!['models','limits','classify','overwrite_exceptions'].every(key=>typeof payload[key]==='boolean')||!payload.models&&!payload.limits&&!payload.classify)fail(400,'请选择应用类型、范围与配置项目');
    const scope=payload.scope;if(!fields(scope,['type_id','member_ids','member_versions'])||(Object.hasOwn(scope,'type_id')===Object.hasOwn(scope,'member_ids'))||scope.type_id!==undefined&&typeof scope.type_id!=='string'||scope.member_ids!==undefined&&(!Array.isArray(scope.member_ids)||!scope.member_ids.length||scope.member_ids.length>maxTargets||scope.member_ids.some(value=>!uuid(value))||new Set(scope.member_ids).size!==scope.member_ids.length))fail(400,'批量目标须为一个完整类型或明确的员工ID');
    // Selection includes access metadata for access batches. Configuration
    // batches validate it but compare only their own configuration versions.
    if(scope.member_versions!==undefined&&(!scope.member_ids||!Array.isArray(scope.member_versions)||scope.member_versions.length!==scope.member_ids.length||new Set(scope.member_versions.map(row=>row?.id)).size!==scope.member_ids.length||scope.member_versions.some(row=>!fields(row,['id','usage_type_version','defaults_version','access_version'])||row.access_version!==undefined&&(!Number.isSafeInteger(row.access_version)||row.access_version<0)||!scope.member_ids.includes(row.id)||!Number.isSafeInteger(row.usage_type_version)||row.usage_type_version<0||!Number.isSafeInteger(row.defaults_version)||row.defaults_version<0)))fail(400,'选择的员工配置版本无效');
    return store.transaction(async()=>{
      await departments.current(authorization,{lock:true,write:true});
      const target=await type(store,payload.type_id,authorization);if(target.version!==payload.expected_type_version)fail(409,'类型默认已变化，请重新预览','member_type_conflict');
      if((payload.models||payload.limits)&&target.default_version===null)fail(409,'目标类型默认尚未配置','member_type_template_conflict');
      if(payload.limits)await limits.validate(target.usage_limits);
      if(scope.type_id!==undefined&&!await store.get('SELECT id FROM harbor.member_types WHERE id=$1'+(authorization?' AND department_id=$2':''),scope.type_id,...(authorization?[authorization.department_id]:[])))fail(400,'范围类型不存在');
      const members=await store.all('SELECT id FROM harbor.members WHERE removed_at IS NULL AND '+(scope.type_id!==undefined?'usage_type_id=$1':'id=ANY($1::text[])')+(authorization?' AND department_id=$3':'')+' ORDER BY id LIMIT $2',scope.type_id??scope.member_ids,maxTargets+1,...(authorization?[authorization.department_id]:[]));
      if(!members.length)fail(400,'没有符合范围的员工');if(members.length>maxTargets)fail(413,'目标超过批量配置容量，请缩小范围','member_batch_capacity');
      if(scope.member_ids&&members.length!==scope.member_ids.length)fail(409,'部分员工已删除，请重新选择','member_batch_conflict');
      const batchId=id(),at=clock(),operation={type_id:target.id,type_version:target.version,template_version:target.default_version,models:payload.models,limits:payload.limits,classify:payload.classify,overwrite_exceptions:payload.overwrite_exceptions};
      await store.run('INSERT INTO harbor.member_config_batches(id,created,expires,actor,scope,operation'+departments.columns(authorization)+') VALUES($1,$2,$3,$4,$5::jsonb,$6::jsonb'+departments.placeholders(authorization,7)+')',batchId,at,at+ttlMs,'admin',JSON.stringify(scope),JSON.stringify(operation),...departments.values(authorization));
      let size=0,ordinal=0;
      for(const member of members){
        const current=await snapshot(store,member.id,{departmentScope:authorization}),before=current.configuration,desired=structuredClone(before),exceptions=[];
        const selected=scope.member_versions?.find(row=>row.id===member.id);if(selected&&(selected.usage_type_version!==before.usage_type_version||selected.defaults_version!==before.defaults_version))fail(409,'选中员工的分类或配置已变化，请重新选择并预览','member_batch_selection_changed');
        const source={kind:'type',type_id:target.id,template_version:target.default_version};
        if(payload.classify&&before.usage_type_id!==target.id){desired.usage_type_id=target.id;desired.usage_type_label=target.code+' · '+target.name;for(const component of ['models','limits'])desired.configuration_source[component]={...before.configuration_source[component],kind:'retained'};}
        if(payload.models){
          if(!inherited(before.configuration_source.models))exceptions.push({component:'models',action:payload.overwrite_exceptions?'overwritten':'preserved',source:before.configuration_source.models.kind});
          if(payload.overwrite_exceptions||inherited(before.configuration_source.models)){desired.default_models=target.allowed_models;desired.default_models_set=1;desired.configuration_source.models=source;}
          for(const key of desired.keys){
            const follows=inherited(key.model_source)&&inherited(before.configuration_source.models),exceptionSource=inherited(key.model_source)&&!inherited(before.configuration_source.models)?before.configuration_source.models.kind:key.model_source.kind;
            if(!follows)exceptions.push({component:'key',key_id:key.id,action:payload.overwrite_exceptions?'overwritten':'preserved',source:exceptionSource});
            if(payload.overwrite_exceptions||follows){key.allowed_models=target.allowed_models;key.model_source={...source,kind:'employee'};}
          }
        }
        if(payload.limits){
          if(!inherited(before.configuration_source.limits))exceptions.push({component:'limits',action:payload.overwrite_exceptions?'overwritten':'preserved',source:before.configuration_source.limits.kind});
          if(payload.overwrite_exceptions||inherited(before.configuration_source.limits)){desired.usage_limits={...target.usage_limits,version:before.usage_limits.version};desired.configuration_source.limits=source;}
        }
        const encoded=[before,desired,exceptions].map(value=>JSON.stringify(value)),itemBytes=encoded.reduce((sum,value)=>sum+Buffer.byteLength(value),0);size+=itemBytes;if(itemBytes>pageBytes/2||size>maxPreviewBytes)fail(413,'完整预览超过容量，未保存部分目标','member_batch_capacity');
        await store.run('INSERT INTO harbor.member_config_batch_items(batch_id,ordinal,member_id,member_name,before_config,desired_config,exceptions) VALUES($1,$2,$3,$4,$5::jsonb,$6::jsonb,$7::jsonb)',batchId,++ordinal,member.id,current.name,...encoded);
      }
      return {id:batchId,status:'preview',members:ordinal,expires:at+ttlMs};
    },{level:'REPEATABLE READ'});
  }
  async function batch(target,batchId,{lock=false,departmentScope=null}={}){
    if(!uuid(batchId))fail(404,'配置批次不存在','member_batch_not_found');const at=clock(),window=usageWindow(at);
    const value=await target.get('SELECT * FROM harbor.member_config_batches WHERE id=$1 AND created>=$2 AND created<$3'+(departmentScope?.view==='department'?' AND department_id=$4':'')+(lock?' FOR UPDATE':''),batchId,window.start,window.end,...(departmentScope?.view==='department'?[departmentScope.department_id]:[]));
    if(!value)fail(404,'配置批次不存在或超出查询范围','member_batch_not_found');return value;
  }
  async function status(batchId,{after=0,limit=25,signal,scope}={}){
    const authorization=await departments.resolve(scope);
    if(!Number.isSafeInteger(after)||after<0||!Number.isSafeInteger(limit)||limit<1||limit>100)fail(400,'配置批次分页无效');
    return read(()=>readStore.transaction(async()=>{
      const value=await batch(readStore,batchId,{departmentScope:authorization}),counts=await readStore.all('SELECT status,COUNT(*) count FROM harbor.member_config_batch_items WHERE batch_id=$1 GROUP BY status',batchId),rows=await readStore.all('SELECT ordinal,member_id,member_name,before_config,desired_config,exceptions,status,reason,after_config FROM harbor.member_config_batch_items WHERE batch_id=$1 AND ordinal>$2 ORDER BY ordinal LIMIT $3',batchId,after,limit+1);
      const items=[];let bytes=0;for(const row of rows.slice(0,limit)){const size=Buffer.byteLength(JSON.stringify(row));if(size>pageBytes)fail(503,'单员工配置结果超过分页容量','member_batch_capacity');if(items.length&&bytes+size>pageBytes)break;items.push(row);bytes+=size;}
      const execution=value.job_id?await readStore.get('SELECT status,error_code FROM harbor.background_jobs WHERE id=$1',value.job_id):null;
      return {...value,job_status:execution?.status??null,execution_failed:value.status==='queued'&&['failed','cancelled'].includes(execution?.status),counts:Object.fromEntries(counts.map(row=>[row.status,row.count])),items,after:items.at(-1)?.ordinal??after,has_more:rows.length>items.length};
    },{level:'REPEATABLE READ',readOnly:true}),{signal});
  }
  async function submit(batchId,{scope}={}){
    const authorization=await departments.resolve(scope,{write:true});
    return store.transaction(async()=>{
      await departments.current(authorization,{lock:true,write:true});
      const value=await batch(store,batchId,{lock:true,departmentScope:authorization});if(value.status!=='preview')return {id:batchId,status:value.status,job_id:value.job_id};
      if(value.expires<=clock())fail(410,'预览已过期，请重新生成','member_batch_expired');
      departments.owner(authorization,value);
      const result=await jobs.enqueue('member.config',{id:'member.config:'+batchId,target:batchId,payload:{batch_id:batchId},scope:authorization??undefined});
      await store.run("UPDATE harbor.member_config_batches SET status='queued',job_id=$1 WHERE id=$2",result.id,batchId);return {id:batchId,status:'queued',job_id:result.id};
    });
  }
  async function cancel(batchId,{scope}={}){const authorization=await departments.resolve(scope,{write:true});return store.transaction(async()=>{
      await departments.current(authorization,{lock:true,write:true});
    const value=await batch(store,batchId,{lock:true,departmentScope:authorization});if(['completed','cancelled'].includes(value.status))return {id:batchId,status:value.status};
    const at=clock();await store.run("UPDATE harbor.member_config_batch_items SET status='cancelled',reason='admin_cancelled',finished=$1 WHERE batch_id=$2 AND status='pending'",at,batchId);await store.run("UPDATE harbor.member_config_batches SET status='cancelled',finished=$1 WHERE id=$2",at,batchId);return {id:batchId,status:'cancelled'};
  });}
  async function derived(batchId,direction,{scope}={}){const authorization=await departments.resolve(scope,{write:true});return store.transaction(async()=>{
      await departments.current(authorization,{lock:true,write:true});
    const previous=await batch(store,batchId,{lock:true,departmentScope:authorization});
    if(previous.status==='queued'&&['failed','cancelled'].includes((await store.get('SELECT status FROM harbor.background_jobs WHERE id=$1',previous.job_id))?.status)){
      await store.run("UPDATE harbor.member_config_batch_items SET status='failed',reason='execution_interrupted',finished=$1 WHERE batch_id=$2 AND status='pending'",clock(),batchId);
      await store.run("UPDATE harbor.member_config_batches SET status='completed',finished=$1 WHERE id=$2",clock(),batchId);previous.status='completed';
    }
    if(!['completed','cancelled'].includes(previous.status))fail(409,'原批次尚未结束');
    if(direction==='undo'&&previous.direction==='undo')fail(400,'撤销批次不能再次撤销');
    const items=await store.all('SELECT * FROM harbor.member_config_batch_items WHERE batch_id=$1 AND status=$2 ORDER BY ordinal',batchId,direction==='undo'?'success':'failed');
    if(!items.length){if(direction==='retry')return {id:batchId,status:previous.status,no_retry_needed:true};fail(409,'没有可撤销的已应用员工');}
    const next=id(),at=clock();await store.run('INSERT INTO harbor.member_config_batches(id,created,expires,actor,scope,operation,parent_id,direction'+departments.columns(authorization)+') VALUES($1,$2,$3,$4,$5::jsonb,$6::jsonb,$7,$8'+departments.placeholders(authorization,9)+')',next,at,at+ttlMs,'admin',JSON.stringify({member_ids:items.map(item=>item.member_id)}),JSON.stringify(previous.operation),batchId,direction==='undo'?'undo':previous.direction,...departments.values(authorization));
    for(const [index,item] of items.entries())await store.run('INSERT INTO harbor.member_config_batch_items(batch_id,ordinal,member_id,member_name,before_config,desired_config,exceptions) VALUES($1,$2,$3,$4,$5::jsonb,$6::jsonb,$7::jsonb)',next,index+1,item.member_id,item.member_name,JSON.stringify(direction==='undo'?item.after_config:item.before_config),JSON.stringify(direction==='undo'?item.before_config:item.desired_config),JSON.stringify(item.exceptions));
    return {id:next,status:'preview',members:items.length,expires:at+ttlMs};
  });}
  async function apply(memberId,current,desired,authorization){
    if(authorization){if(desired.id!==memberId||desired.keys.length!==current.keys.length||desired.keys.some(key=>!current.keys.some(old=>old.id===key.id)))fail(403,'批量配置目标与部门不匹配','department_batch_owner');if(desired.usage_type_id&&!await store.get('SELECT id FROM harbor.member_types WHERE id=$1 AND department_id=$2',desired.usage_type_id,authorization.department_id))fail(403,'使用类型不属于当前部门','department_batch_owner');}
    const nextSource=desired.configuration_source,modelsChanged=!equal([current.default_models,current.default_models_set,current.configuration_source.models],[desired.default_models,desired.default_models_set,nextSource.models]),limitValuesChanged=!equal([current.usage_limits.enabled,current.usage_limits.rules],[desired.usage_limits.enabled,desired.usage_limits.rules]),typeChanged=current.usage_type_id!==desired.usage_type_id;
    if(limitValuesChanged)await limits.save(memberId,{enabled:desired.usage_limits.enabled,rules:desired.usage_limits.rules,expected_version:current.usage_limits.version},{source:nextSource.limits,scope:authorization??undefined});
    else if(!equal(current.configuration_source.limits,nextSource.limits))await store.run("UPDATE harbor.members SET configuration_source=jsonb_set(configuration_source,'{limits}',$1::jsonb),defaults_version=defaults_version+1 WHERE id=$2",JSON.stringify(nextSource.limits),memberId);
    if(modelsChanged||typeChanged)await store.run('UPDATE harbor.members SET usage_type_id=$1,usage_type_version=usage_type_version+$2,default_models=$3,default_models_set=$4,configuration_source=jsonb_set(configuration_source,\'{models}\',$5::jsonb),defaults_version=defaults_version+1 WHERE id=$6',desired.usage_type_id,typeChanged?1:0,desired.default_models===null?null:JSON.stringify(desired.default_models),desired.default_models_set,JSON.stringify(nextSource.models),memberId);
    for(const key of desired.keys){const old=current.keys.find(value=>value.id===key.id);if(!equal([old.allowed_models,old.model_source],[key.allowed_models,key.model_source]))await store.run('UPDATE harbor.keys SET allowed_models=$1,model_source=$2::jsonb,model_policy_version=model_policy_version+1 WHERE id=$3 AND member_id=$4',key.allowed_models===null?null:JSON.stringify(key.allowed_models),JSON.stringify(key.model_source),key.id,memberId);}
  }
  async function execute(job,{signal}={}){
    const batchId=job.payload?.batch_id;if(job.kind!=='member.config'||!uuid(batchId)||job.target!==batchId||job.id!=='member.config:'+batchId||Object.keys(job.payload).length!==1)fail(400,'配置任务参数无效');
    for(;;){
      if(signal?.aborted)fail(503,'配置任务已停止','worker_stopping');
      const item=await jobs.owned(job,async()=>{await departments.job(job,batchId);return store.get("SELECT ordinal FROM harbor.member_config_batch_items WHERE batch_id=$1 AND status='pending' ORDER BY ordinal LIMIT 1",batchId);});
      if(!item)break;
      try{await jobs.owned(job,async()=>{
        const authorization=await departments.job(job,batchId);
        const value=await store.get('SELECT * FROM harbor.member_config_batches WHERE id=$1 FOR UPDATE',batchId),entry=await store.get('SELECT * FROM harbor.member_config_batch_items WHERE batch_id=$1 AND ordinal=$2 FOR UPDATE',batchId,item.ordinal);if(value.status!=='queued'||entry.status!=='pending')return;
        const actual=await snapshot(store,entry.member_id,{lock:true,departmentScope:authorization});let reason=actual?equal(actual.configuration,entry.before_config)?null:'configuration_changed':'member_removed';
        if(!reason&&value.direction==='apply'){
          const target=await store.get('SELECT enabled,version,default_version FROM harbor.member_types WHERE id=$1'+(authorization?' AND department_id=$2':'')+' FOR SHARE',value.operation.type_id,...(authorization?[authorization.department_id]:[]));
          if(!target?.enabled||target.version!==value.operation.type_version||target.default_version!==value.operation.template_version)reason='template_changed';
        }
        if(reason){await store.run("UPDATE harbor.member_config_batch_items SET status='skipped',reason=$1,finished=$2 WHERE batch_id=$3 AND ordinal=$4",reason,clock(),batchId,item.ordinal);return;}
        if(signal?.aborted)fail(503,'配置任务已停止','worker_stopping');
        const unchanged=equal(entry.before_config,entry.desired_config);if(!unchanged)await apply(entry.member_id,actual.configuration,entry.desired_config,authorization);
        const after=(await snapshot(store,entry.member_id,{departmentScope:authorization})).configuration;
        await createPostgresAudit(store,{clock}).write(authorization,{action:'member.batch.apply',detail:{member:entry.member_id,batch:batchId,direction:value.direction,actor:value.actor},subject:{kind:'member',id:entry.member_id}});
        await store.run('UPDATE harbor.member_config_batch_items SET status=$1,after_config=$2::jsonb,finished=$3 WHERE batch_id=$4 AND ordinal=$5',unchanged?'unchanged':'success',JSON.stringify(after),clock(),batchId,item.ordinal);
      });}catch(error){
        if(error.code==='job_lease_lost'||error.status===403||signal?.aborted)throw error;
        await jobs.owned(job,async()=>{await departments.job(job,batchId);return store.run("UPDATE harbor.member_config_batch_items SET status='failed',reason='configuration_failed',finished=$1 WHERE batch_id=$2 AND ordinal=$3 AND status='pending'",clock(),batchId,item.ordinal);});
      }
    }
    return jobs.commitEffect(job,async()=>{
      await departments.job(job,batchId);
      await store.run("UPDATE harbor.member_config_batches SET status='completed',finished=$1 WHERE id=$2 AND status='queued'",clock(),batchId);
      const counts=await store.all('SELECT status,COUNT(*) count FROM harbor.member_config_batch_items WHERE batch_id=$1 GROUP BY status',batchId);return {id:batchId,counts:Object.fromEntries(counts.map(row=>[row.status,row.count]))};
    });
  }
  async function handle(path,method,payload,params=new URLSearchParams(),{signal,scope}={}){
    if(path==='/api/admin/member-config-batches'&&method==='GET'){
      const authorization=await departments.resolve(scope);
      if([...params.keys()].some(key=>!['before','limit'].includes(key)||params.getAll(key).length!==1)||params.has('before')&&!uuid(params.get('before'))||params.has('limit')&&!/^\d+$/.test(params.get('limit')))fail(400,'配置记录分页无效');
      const limit=Number(params.get('limit')??25);if(!Number.isSafeInteger(limit)||limit<1||limit>100)fail(400,'配置记录分页无效');
      return {status:200,value:await read(()=>readStore.transaction(async()=>{
        const window=usageWindow(clock()),cursor=params.has('before')?await batch(readStore,params.get('before'),{departmentScope:authorization}):null,rows=await readStore.all(`SELECT b.id,b.created,b.status,b.direction,b.parent_id,b.operation,
          (SELECT COUNT(*) FROM harbor.member_config_batch_items i WHERE i.batch_id=b.id) members
          FROM harbor.member_config_batches b WHERE ${authorization?.view==='department'?'b.department_id=$6 AND ':''}b.created>=$1 AND b.created<$2 AND ($3::bigint IS NULL OR (b.created,b.id)<($3,$4)) ORDER BY b.created DESC,b.id DESC LIMIT $5`,window.start,window.end,cursor?.created??null,cursor?.id??'',limit+1,...(authorization?.view==='department'?[authorization.department_id]:[]));
        const items=rows.slice(0,limit);return {items,before:items.at(-1)?.id??null,has_more:rows.length>limit};
      },{level:'REPEATABLE READ',readOnly:true}),{signal})};
    }
    if(path==='/api/admin/member-config-batches/preview'&&method==='POST')return {status:201,value:await preview(payload,{scope})};
    const match=path.match(/^\/api\/admin\/member-config-batches\/([a-f0-9-]{36})(?:\/(execute|retry|undo))?$/);if(!match)return null;
    if(method==='GET'&&!match[2]){if([...params.keys()].some(key=>!['after','limit'].includes(key)||params.getAll(key).length!==1)||['after','limit'].some(key=>params.has(key)&&!/^\d+$/.test(params.get(key))))fail(400,'配置批次分页无效');return {status:200,value:await status(match[1],{signal,scope,after:Number(params.get('after')??0),limit:Number(params.get('limit')??25)})};}
    if(method==='POST'&&match[2]){if(!fields(payload,[])||Object.keys(payload).length)fail(400,'操作不接受替换预览目标');return {status:match[2]==='execute'?202:201,value:await (match[2]==='execute'?submit(match[1],{scope}):derived(match[1],match[2],{scope}))};}
    if(method==='DELETE'&&!match[2])return {status:200,value:await cancel(match[1],{scope})};return null;
  }
  return {preview,status,submit,cancel,derived,handlers:{'member.config':execute},handle};
}
