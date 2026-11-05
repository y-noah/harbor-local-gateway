import {isDeepStrictEqual as equal} from 'node:util';
import {AppError,id,usageWindow} from './store.mjs';
import {createPostgresJobs} from './postgres-jobs.mjs';

const fail=(status,message,code='member_action_invalid')=>{throw new AppError(status,message,code);};
const entityId=value=>typeof value==='string'&&/^[a-zA-Z0-9-]{1,80}$/.test(value);
const uuid=value=>typeof value==='string'&&/^[a-f0-9]{8}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{12}$/.test(value);
const object=(value,fields)=>value&&typeof value==='object'&&!Array.isArray(value)&&Object.keys(value).every(field=>fields.includes(field));
const version=value=>Number.isSafeInteger(value)&&value>=0;
const stopped=signal=>{if(signal?.aborted)fail(503,'批量访问操作已停止','worker_stopping');};

// Access mutations use the same member -> Key lock order as normal management
// and admission. Metadata in the preview never includes a Key secret or digest.
export function createPostgresMemberActions(store,{jobs=createPostgresJobs(store),readStore=store,queryQueue,clock=Date.now,maxMembers=1000,maxKeys=10000,maxKeysPerMember=1000,maxPreviewBytes=4194304,pageBytes=262144,ttlMs=3600000}={}){
  if(!Number.isInteger(maxMembers)||maxMembers<1||maxMembers>10000||!Number.isInteger(maxKeys)||maxKeys<1||maxKeys>100000||!Number.isInteger(maxKeysPerMember)||maxKeysPerMember<1||maxKeysPerMember>10000||!Number.isInteger(maxPreviewBytes)||maxPreviewBytes<1024||maxPreviewBytes>67108864||!Number.isInteger(pageBytes)||pageBytes<1024||pageBytes>2097152||!Number.isInteger(ttlMs)||ttlMs<60000||ttlMs>86400000)fail(503,'批量访问操作资源配置无效','member_action_configuration');
  const read=op=>queryQueue?queryQueue.run(op):op();
  const ids=(value,max,label)=>{if(!Array.isArray(value)||!value.length||value.length>max||value.some(item=>!entityId(item))||new Set(value).size!==value.length)fail(400,label+'须为不重复的明确ID列表');return [...value].sort();};
  async function snapshot(target,memberId,{lock=false,keys=null}={}){
    const owner=await target.get('SELECT id,enabled,access_version,usage_type_id,usage_type_version,defaults_version FROM harbor.members WHERE id=$1 AND removed_at IS NULL'+(lock?' FOR UPDATE':''),memberId);if(!owner)return null;
    const rows=await target.all(`SELECT id,member_id,enabled,expires,access_version,model_policy_version FROM harbor.keys WHERE member_id=$1 AND removed_at IS NULL${keys?' AND id=ANY($3::text[])':''} ORDER BY id LIMIT $2${lock?' FOR UPDATE':''}`,memberId,maxKeysPerMember+1,...(keys?[keys]:[]));
    if(rows.length>maxKeysPerMember)fail(413,'单名员工的完整Key预览超过容量','member_action_capacity');
    const collection=keys?await target.all('SELECT id FROM harbor.keys WHERE member_id=$1 AND removed_at IS NULL ORDER BY id LIMIT $2',memberId,maxKeysPerMember+1):rows;if(collection.length>maxKeysPerMember)fail(413,'员工完整Key集合超过容量','member_action_capacity');
    return {member:owner,keys:rows,key_set:collection.map(row=>row.id)};
  }
  async function getBatch(target,batchId,{lock=false}={}){if(!uuid(batchId))fail(404,'访问操作批次不存在','member_action_not_found');const window=usageWindow(clock()),value=await target.get('SELECT * FROM harbor.member_action_batches WHERE id=$1 AND created>=$2 AND created<$3'+(lock?' FOR UPDATE':''),batchId,window.start,window.end);if(!value)fail(404,'访问操作批次不存在或超出近7天范围','member_action_not_found');return value;}
  async function preview(payload){
    if(!object(payload,['kind','enabled','expires','member_ids','key_ids','member_versions','key_versions'])||!['member_access','key_renewal'].includes(payload.kind))fail(400,'请选择员工启停或选定Key续期');
    const employeeAction=payload.kind==='member_access';let memberIds,keyIds,operation;
    if(employeeAction){if(typeof payload.enabled!=='boolean'||payload.expires!==undefined||payload.key_ids!==undefined||payload.key_versions!==undefined)fail(400,'员工访问操作须明确启用或停用');memberIds=ids(payload.member_ids,maxMembers,'员工');operation={kind:payload.kind,enabled:payload.enabled};}
    else{if(payload.enabled!==undefined||payload.member_ids!==undefined||payload.member_versions!==undefined||!Object.hasOwn(payload,'expires')||payload.expires!==null&&(!Number.isSafeInteger(payload.expires)||payload.expires<=clock()||payload.expires>8640000000000000))fail(400,'续期须明确有效的未来到期时间或永久');keyIds=ids(payload.key_ids,maxKeys,'Key');operation={kind:payload.kind,expires:payload.expires,mode:'extend_only'};}
    const selectedVersions=employeeAction?payload.member_versions:payload.key_versions,selectedIds=employeeAction?memberIds:keyIds;
    if(selectedVersions!==undefined&&(!Array.isArray(selectedVersions)||selectedVersions.length!==selectedIds.length||new Set(selectedVersions.map(row=>row?.id)).size!==selectedIds.length||selectedVersions.some(row=>!object(row,employeeAction?['id','access_version','usage_type_version','defaults_version']:['id','access_version','model_policy_version'])||!selectedIds.includes(row.id)||!version(row.access_version)||employeeAction&&(!version(row.usage_type_version)||!version(row.defaults_version))||!employeeAction&&!version(row.model_policy_version))))fail(400,'选中对象版本无效');
    return store.transaction(async()=>{
      let owners;
      if(employeeAction)owners=await store.all('SELECT id,name FROM harbor.members WHERE id=ANY($1::text[]) AND removed_at IS NULL ORDER BY id',memberIds);
      else{const selected=await store.all('SELECT k.id,k.member_id,m.name FROM harbor.keys k JOIN harbor.members m ON m.id=k.member_id WHERE k.id=ANY($1::text[]) AND k.removed_at IS NULL AND m.removed_at IS NULL ORDER BY k.member_id,k.id',keyIds);if(selected.length!==keyIds.length)fail(409,'部分Key或员工已删除，请重新选择','member_action_selection_changed');owners=[...new Map(selected.map(key=>[key.member_id,{id:key.member_id,name:key.name}])).values()];}
      if(!owners.length||owners.length>maxMembers||employeeAction&&owners.length!==memberIds.length)fail(409,'部分员工已移除或目标超出容量，请重新选择','member_action_selection_changed');
      const batchId=id(),at=clock();await store.run('INSERT INTO harbor.member_action_batches(id,created,expires,actor,operation) VALUES($1,$2,$3,$4,$5::jsonb)',batchId,at,at+ttlMs,'admin',JSON.stringify(operation));let bytes=0,keyCount=0;
      for(const [index,owner] of owners.entries()){
        const before=await snapshot(store,owner.id,{keys:employeeAction?null:keyIds}),desired=structuredClone(before),notes=[];
        if(employeeAction){const selected=selectedVersions?.find(row=>row.id===owner.id);if(selected&&!equal(selected,{id:owner.id,access_version:before.member.access_version,usage_type_version:before.member.usage_type_version,defaults_version:before.member.defaults_version}))fail(409,'员工访问、类型或配置已变化，请重新选择','member_action_selection_changed');desired.member.enabled=payload.enabled?1:0;}
        else for(const key of desired.keys){const selected=selectedVersions?.find(row=>row.id===key.id);if(selected&&!equal(selected,{id:key.id,access_version:key.access_version,model_policy_version:key.model_policy_version}))fail(409,'选中Key已变化，请重新选择','member_action_selection_changed');const previous=key.expires;
          if(previous===null)notes.push({key_id:key.id,reason:'already_permanent'});else if(payload.expires!==null&&previous>=payload.expires)notes.push({key_id:key.id,reason:previous===payload.expires?'same_expiry':'already_longer'});else{key.expires=payload.expires;notes.push({key_id:key.id,reason:payload.expires===null?'make_permanent':'extend'});}
        }
        keyCount+=before.keys.length;if(keyCount>maxKeys)fail(413,'完整Key预览超过批次容量','member_action_capacity');
        const encoded=[before,desired,notes].map(value=>JSON.stringify(value)),size=encoded.reduce((total,value)=>total+Buffer.byteLength(value),0);bytes+=size;if(size>pageBytes/2||bytes>maxPreviewBytes)fail(413,'完整访问操作预览超过容量，未保存部分目标','member_action_capacity');
        await store.run('INSERT INTO harbor.member_action_items(batch_id,ordinal,member_id,member_name,selected_keys,before_state,desired_state,notes) VALUES($1,$2,$3,$4,$5::jsonb,$6::jsonb,$7::jsonb,$8::jsonb)',batchId,index+1,owner.id,owner.name,employeeAction?null:JSON.stringify(before.keys.map(key=>key.id)),...encoded);
      }
      return {id:batchId,status:'preview',members:owners.length,keys:keyCount,operation,expires:at+ttlMs};
    },{level:'REPEATABLE READ'});
  }
  async function detail(batchId,{after=0,limit=25}={}){
    if(!Number.isSafeInteger(after)||after<0||!Number.isInteger(limit)||limit<1||limit>100)fail(400,'访问操作分页无效');
    return read(()=>readStore.transaction(async()=>{
      const batch=await getBatch(readStore,batchId),counts=await readStore.all('SELECT status,COUNT(*) count FROM harbor.member_action_items WHERE batch_id=$1 GROUP BY status',batchId),rows=await readStore.all('SELECT ordinal,member_id,member_name,selected_keys,before_state,desired_state,notes,status,reason,after_state FROM harbor.member_action_items WHERE batch_id=$1 AND ordinal>$2 ORDER BY ordinal LIMIT $3',batchId,after,limit+1),items=[];let bytes=0;
      for(const row of rows.slice(0,limit)){const size=Buffer.byteLength(JSON.stringify(row));if(size>pageBytes)fail(503,'单名员工结果超过页面容量','member_action_capacity');if(items.length&&bytes+size>pageBytes)break;items.push(row);bytes+=size;}
      const job=batch.job_id?await readStore.get('SELECT status,error_code FROM harbor.background_jobs WHERE id=$1',batch.job_id):null;return {...batch,counts:Object.fromEntries(counts.map(row=>[row.status,row.count])),items,after:items.at(-1)?.ordinal??after,has_more:rows.length>items.length,job_status:job?.status??null,execution_failed:batch.status==='queued'&&['failed','cancelled'].includes(job?.status)};
    },{level:'REPEATABLE READ',readOnly:true}));
  }
  async function submit(batchId){return store.transaction(async()=>{
    const batch=await getBatch(store,batchId,{lock:true});if(batch.status!=='preview')return {id:batchId,status:batch.status,job_id:batch.job_id};if(batch.expires<=clock())fail(410,'访问操作预览已过期，请重新生成','member_action_expired');const job=await jobs.enqueue('member.actions',{id:'member.actions:'+batchId,target:batchId,payload:{batch_id:batchId}});await store.run("UPDATE harbor.member_action_batches SET status='queued',job_id=$1 WHERE id=$2",job.id,batchId);return {id:batchId,status:'queued',job_id:job.id};
  });}
  async function cancel(batchId){return store.transaction(async()=>{
    const batch=await getBatch(store,batchId,{lock:true});if(['completed','cancelled'].includes(batch.status))return {id:batchId,status:batch.status};await store.run("UPDATE harbor.member_action_items SET status='cancelled',reason='admin_cancelled',finished=$1 WHERE batch_id=$2 AND status='pending'",clock(),batchId);await store.run("UPDATE harbor.member_action_batches SET status='cancelled',finished=$1 WHERE id=$2",clock(),batchId);return {id:batchId,status:'cancelled'};
  });}
  async function derived(batchId,kind){
    if(!['retry','undo'].includes(kind))fail(400,'衍生访问操作无效');return store.transaction(async()=>{
      const batch=await getBatch(store,batchId,{lock:true});
      if(batch.status==='queued'&&['failed','cancelled'].includes((await store.get('SELECT status FROM harbor.background_jobs WHERE id=$1',batch.job_id))?.status)){await store.run("UPDATE harbor.member_action_items SET status='failed',reason='execution_interrupted',finished=$1 WHERE batch_id=$2 AND status='pending'",clock(),batchId);await store.run("UPDATE harbor.member_action_batches SET status='completed',finished=$1 WHERE id=$2",clock(),batchId);batch.status='completed';}
      if(!['completed','cancelled'].includes(batch.status))fail(409,'原批次尚未结束');if(kind==='undo'&&batch.direction==='undo')fail(400,'撤销批次不能再次撤销');
      const items=await store.all('SELECT * FROM harbor.member_action_items WHERE batch_id=$1 AND status=$2 ORDER BY ordinal',batchId,kind==='undo'?'success':'failed');if(!items.length){if(kind==='retry')return {id:batchId,status:batch.status,no_retry_needed:true};fail(409,'没有已应用的员工可撤销');}
      const next=id(),at=clock(),direction=kind==='undo'?'undo':batch.direction;await store.run('INSERT INTO harbor.member_action_batches(id,created,expires,actor,operation,direction,parent_id) VALUES($1,$2,$3,$4,$5::jsonb,$6,$7)',next,at,at+ttlMs,'admin',JSON.stringify(batch.operation),direction,batchId);
      for(const [index,item] of items.entries())await store.run('INSERT INTO harbor.member_action_items(batch_id,ordinal,member_id,member_name,selected_keys,before_state,desired_state,notes) VALUES($1,$2,$3,$4,$5::jsonb,$6::jsonb,$7::jsonb,$8::jsonb)',next,index+1,item.member_id,item.member_name,JSON.stringify(item.selected_keys),JSON.stringify(kind==='undo'?item.after_state:item.before_state),JSON.stringify(kind==='undo'?item.before_state:item.desired_state),JSON.stringify(item.notes));
      return {id:next,status:'preview',members:items.length,direction,expires:at+ttlMs};
    });
  }
  async function execute(job,{signal}={}){
    const batchId=job.payload?.batch_id;if(job.kind!=='member.actions'||!uuid(batchId)||job.id!=='member.actions:'+batchId||job.target!==batchId||!object(job.payload,['batch_id'])||Object.keys(job.payload).length!==1)fail(400,'批量访问任务参数无效');
    for(;;){stopped(signal);const item=await jobs.owned(job,()=>store.get("SELECT ordinal FROM harbor.member_action_items WHERE batch_id=$1 AND status='pending' ORDER BY ordinal LIMIT 1",batchId));if(!item)break;
      try{await jobs.owned(job,async()=>{
        const batch=await store.get('SELECT * FROM harbor.member_action_batches WHERE id=$1 FOR UPDATE',batchId),entry=await store.get('SELECT * FROM harbor.member_action_items WHERE batch_id=$1 AND ordinal=$2 FOR UPDATE',batchId,item.ordinal);if(batch.status!=='queued'||entry.status!=='pending')return;
        const current=await snapshot(store,entry.member_id,{lock:true,keys:entry.selected_keys}),same=current&&(batch.direction==='apply'&&batch.operation.kind==='key_renewal'?equal({...current,key_set:null},{...entry.before_state,key_set:null}):equal(current,entry.before_state)),reason=!current?'member_removed':!same?'state_changed':batch.direction==='apply'&&batch.operation.kind==='key_renewal'&&batch.operation.expires!==null&&batch.operation.expires<=clock()?'renewal_target_expired':null;
        if(reason){await store.run("UPDATE harbor.member_action_items SET status='skipped',reason=$1,finished=$2 WHERE batch_id=$3 AND ordinal=$4",reason,clock(),batchId,item.ordinal);return;}stopped(signal);
        let changed=false;if(current.member.enabled!==entry.desired_state.member.enabled){await store.run('UPDATE harbor.members SET enabled=$1 WHERE id=$2',entry.desired_state.member.enabled,entry.member_id);changed=true;}
        for(const key of entry.desired_state.keys){const previous=current.keys.find(row=>row.id===key.id);if(previous.expires!==key.expires){await store.run('UPDATE harbor.keys SET expires=$1 WHERE id=$2 AND member_id=$3',key.expires,key.id,entry.member_id);changed=true;}}
        const after=await snapshot(store,entry.member_id,{keys:entry.selected_keys});await store.run('INSERT INTO harbor.audit VALUES($1,$2,$3,$4)',id(),clock(),'member.actions.apply',JSON.stringify({member:entry.member_id,batch:batchId,operation:batch.operation.kind,direction:batch.direction,actor:batch.actor}));
        await store.run('UPDATE harbor.member_action_items SET status=$1,after_state=$2::jsonb,finished=$3 WHERE batch_id=$4 AND ordinal=$5',changed?'success':'unchanged',JSON.stringify(after),clock(),batchId,item.ordinal);
      });}catch(error){if(error.code==='job_lease_lost'||signal?.aborted)throw error;await jobs.owned(job,()=>store.run("UPDATE harbor.member_action_items SET status='failed',reason='operation_failed',finished=$1 WHERE batch_id=$2 AND ordinal=$3 AND status='pending'",clock(),batchId,item.ordinal));}
    }
    return jobs.commitEffect(job,async()=>{await store.run("UPDATE harbor.member_action_batches SET status='completed',finished=$1 WHERE id=$2 AND status='queued'",clock(),batchId);const counts=await store.all('SELECT status,COUNT(*) count FROM harbor.member_action_items WHERE batch_id=$1 GROUP BY status',batchId);return {id:batchId,counts:Object.fromEntries(counts.map(row=>[row.status,row.count]))};});
  }
  async function list(params){
    if([...params.keys()].some(key=>!['before','limit'].includes(key)||params.getAll(key).length!==1)||params.has('before')&&!uuid(params.get('before'))||params.has('limit')&&!/^[1-9]\d*$/.test(params.get('limit')))fail(400,'访问操作记录分页无效');const limit=Number(params.get('limit')??25);if(!Number.isInteger(limit)||limit>100)fail(400,'访问操作记录分页无效');
    return read(()=>readStore.transaction(async()=>{const window=usageWindow(clock()),cursor=params.has('before')?await getBatch(readStore,params.get('before')):null,rows=await readStore.all(`SELECT b.id,b.created,b.status,b.direction,b.parent_id,b.operation,(SELECT COUNT(*) FROM harbor.member_action_items i WHERE i.batch_id=b.id) members FROM harbor.member_action_batches b WHERE b.created>=$1 AND b.created<$2 AND ($3::bigint IS NULL OR (b.created,b.id)<($3,$4)) ORDER BY b.created DESC,b.id DESC LIMIT $5`,window.start,window.end,cursor?.created??null,cursor?.id??'',limit+1),items=rows.slice(0,limit);return {items,before:items.at(-1)?.id??null,has_more:rows.length>limit};},{level:'REPEATABLE READ',readOnly:true}));
  }
  async function handle(path,method,payload,params=new URLSearchParams()){
    if(path==='/api/admin/member-action-batches'&&method==='GET')return {status:200,value:await list(params)};
    if(path==='/api/admin/member-action-batches/preview'&&method==='POST')return {status:201,value:await preview(payload)};
    const match=path.match(/^\/api\/admin\/member-action-batches\/([a-f0-9-]{36})(?:\/(execute|retry|undo))?$/);if(!match)return null;
    if(method==='GET'&&!match[2]){if([...params.keys()].some(key=>!['after','limit'].includes(key)||params.getAll(key).length!==1)||['after','limit'].some(key=>params.has(key)&&!/^\d+$/.test(params.get(key))))fail(400,'访问操作分页无效');return {status:200,value:await detail(match[1],{after:Number(params.get('after')??0),limit:Number(params.get('limit')??25)})};}
    if(method==='POST'&&match[2]){if(!object(payload,[])||Object.keys(payload).length)fail(400,'执行只能确认原固定预览，不能替换目标');return {status:match[2]==='execute'?202:201,value:await (match[2]==='execute'?submit(match[1]):derived(match[1],match[2]))};}
    if(method==='DELETE'&&!match[2])return {status:200,value:await cancel(match[1])};return null;
  }
  return {preview,detail,submit,cancel,derived,list,handle,handlers:{'member.actions':execute}};
}
