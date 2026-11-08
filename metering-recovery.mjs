import {safeUpstreamRequestId} from './request-parameters.mjs';
import {createCipheriv,createDecipheriv,randomBytes,hkdfSync} from 'node:crypto';
import {realpath} from 'node:fs/promises';
import {canonicalChatFacts} from './chat-facts.mjs';
import {AppError} from './store.mjs';
import {RecordingBuffer} from './recording-buffer.mjs';

const statuses=new Set(['ok','failed','unknown','interrupted','cancelled','rejected']);
function facts(command){
  if(!['settle','settle-chat'].includes(command?.type)||typeof command.requestId!=='string'||! /^[a-zA-Z0-9-]{1,100}$/.test(command.requestId)||!Number.isSafeInteger(command.at)||command.at<0||!command.result||typeof command.result!=='object')throw new AppError(503,'计量恢复事实无效','metering_recovery_invalid');
  const result=command.result,status=result.status||'ok';if(!statuses.has(status))throw new AppError(503,'计量恢复状态无效','metering_recovery_invalid');
  let {input=0,output=0,cached=0,usageKnown=true}=result;
  if(![input,output,cached].every(n=>Number.isSafeInteger(n)&&n>=0)||cached>input){input=0;output=0;cached=0;usageKnown=false;}
  const actualModel=typeof result.actualModel==='string'&&/^[a-zA-Z0-9][a-zA-Z0-9._:/-]{0,199}$/.test(result.actualModel)?result.actualModel:undefined;
  const upstreamRequestId=safeUpstreamRequestId(result.upstreamRequestId);
  const chat=canonicalChatFacts(result.chat);if(command.type==='settle-chat'&&chat===undefined)throw new AppError(503,'文本会话恢复游标缺失','metering_recovery_invalid');
  return {type:'settle',requestId:command.requestId,at:command.at,result:{status,input,output,cached,usageKnown:!!usageKnown,...(actualModel===undefined?{}:{actualModel}),...(upstreamRequestId?{upstreamRequestId}:{}),...(chat===undefined?{}:{chat})}};
}

// Same bounded, fsynced spool mechanism, but a separate directory, encryption
// domain and command budget. Recording corruption cannot block billing replay.
export class MeteringRecovery{
  static async create(store,{encryptionKey,...options}={}){
    if(typeof store?.finish!=='function'||!Buffer.isBuffer(encryptionKey)||encryptionKey.length!==32)throw new AppError(503,'计量恢复依赖尚未配置','metering_configuration');
    const key=Buffer.from(hkdfSync('sha256',encryptionKey,Buffer.alloc(0),'Harbor metering recovery v1',32));
    const repository={store,maxBytes:64*1024,
      seal(value,name){const command=facts(value);if(command.result.chat)command.type='settle-chat';const iv=randomBytes(12),cipher=createCipheriv('aes-256-gcm',key,iv);cipher.setAAD(Buffer.from('metering:'+name));return Buffer.concat([iv,cipher.update(JSON.stringify(command),'utf8'),cipher.final(),cipher.getAuthTag()]);},
      open(bytes,name){
        try{const cipher=createDecipheriv('aes-256-gcm',key,bytes.subarray(0,12));cipher.setAAD(Buffer.from('metering:'+name));cipher.setAuthTag(bytes.subarray(-16));return facts(JSON.parse(Buffer.concat([cipher.update(bytes.subarray(12,-16)),cipher.final()]).toString('utf8')));}
        catch{throw new AppError(503,'计量恢复文件无法验证','metering_recovery_unreadable');}
      }};
    const buffer=await RecordingBuffer.create(repository,{maxMemoryBytes:1024*1024,maxDiskBytes:16*1024*1024,maxCommands:1024,maxFiles:8192,...options});
    const directory=await realpath(buffer.directory);return new MeteringRecovery(buffer,directory);
  }
  constructor(buffer,directory){this.buffer=buffer;this.directory=directory;}
  settle(requestId,result,at=Date.now()){
    // Canonicalize before enqueue: no response, tool content or arbitrary error
    // ever enters this queue, even when a caller passes a transport result.
    return this.buffer.enqueue(facts({type:'settle',requestId,result,at}));
  }
  status(){return this.buffer.status();}
  flush(){return this.buffer.flush();}
  close(){return this.buffer.close();}
}
