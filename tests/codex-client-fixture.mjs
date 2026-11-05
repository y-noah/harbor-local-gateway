import {spawn} from 'node:child_process';
export async function runAppServer({binary,cwd,env}){
 const child=spawn(binary,['app-server','--listen','stdio://'],{cwd,env,stdio:['pipe','pipe','pipe']});
 let buffer='',stderr='',next=0;const pending=new Map(),events=[];
 let done,failed;const completed=new Promise((resolve,reject)=>{done=resolve;failed=reject;});completed.catch(()=>{});
 child.stderr.on('data',x=>stderr+=x);child.stdout.on('data',chunk=>{buffer+=chunk;let at;while((at=buffer.indexOf('\n'))!==-1){const line=buffer.slice(0,at);buffer=buffer.slice(at+1);let e;try{e=JSON.parse(line);}catch{continue;}
  if(e.id!==undefined&&pending.has(e.id)){const p=pending.get(e.id);pending.delete(e.id);e.error?p.reject(Error(JSON.stringify(e.error))):p.resolve(e.result);}
  else if(e.method){events.push(e);if(e.method==='turn/completed')done(e.params.turn);}
 }});
 child.on('error',failed);child.on('close',()=>{for(const p of pending.values())p.reject(Error('app-server closed'));failed(Error('app-server closed'));});
 const send=(method,params)=>new Promise((resolve,reject)=>{const id=++next;pending.set(id,{resolve,reject});child.stdin.write(JSON.stringify({id,method,params})+'\n');});
 const timer=setTimeout(()=>child.kill(),45000);
 try{
  await send('initialize',{clientInfo:{name:'harbor_fixture',version:'1.0.0'},capabilities:{experimentalApi:true}});child.stdin.write(JSON.stringify({method:'initialized'})+'\n');
  const {thread}=await send('thread/start',{cwd,approvalPolicy:'never',sandbox:'workspace-write'});
  await send('turn/start',{threadId:thread.id,input:[{type:'text',text:'Exercise fixture tools'}]});
  const turn=await completed;return {code:turn.status==='completed'?0:1,stdout:JSON.stringify(events),stderr};
 }finally{clearTimeout(timer);child.kill();await new Promise(r=>child.exitCode!==null?r():child.once('close',r));}
}
