import {createHmac,hkdfSync,createHash,createCipheriv,createDecipheriv,randomBytes} from 'node:crypto';
import {AppError} from './store.mjs';

const error=()=>new AppError(503,'正文派生数据无法解密，请核对材料和完整性','recording_index_unreadable');
export const normalizeSearch=text=>text.normalize('NFKC').toLocaleLowerCase('en-US');
export function createRecordingIndexCrypto(key){
  if(!Buffer.isBuffer(key)||key.length!==32)throw new AppError(503,'正文后台加密材料无效','recording_index_configuration');
  const encryption=Buffer.from(hkdfSync('sha256',key,Buffer.alloc(0),'harbor-recording-derived-encryption-v1',32)),search=Buffer.from(hkdfSync('sha256',key,Buffer.alloc(0),'harbor-recording-search-v1',32));
  const keyVersion=createHash('sha256').update(search).digest('hex');
  const terms=text=>{const chars=Array.from(normalizeSearch(text)),seen=new Set(),found=new Map();for(let i=1;i<chars.length;i++){const gram=chars[i-1]+chars[i];if(/^\s+$/.test(gram)||seen.has(gram))continue;seen.add(gram);const digest=createHmac('sha256',search).update(gram).digest();found.set(digest.toString('hex'),digest);}return [...found.values()];};
  function seal(value,aad){const iv=randomBytes(12),cipher=createCipheriv('aes-256-gcm',encryption,iv);cipher.setAAD(Buffer.from(aad));return Buffer.concat([iv,cipher.update(JSON.stringify(value),'utf8'),cipher.final(),cipher.getAuthTag()]);}
  function open(value,aad){try{const decipher=createDecipheriv('aes-256-gcm',encryption,value.subarray(0,12));decipher.setAAD(Buffer.from(aad));decipher.setAuthTag(value.subarray(-16));return JSON.parse(Buffer.concat([decipher.update(value.subarray(12,-16)),decipher.final()]).toString('utf8'));}catch{throw error();}}
  return {terms,seal,open,keyVersion};
}

// Assembly preserves source order and field identity. A final replacement replaces
// its deltas; equal text in two different user fields remains two real messages.
export function assembleRecording(events,{maxBytes=2097152,maxFields=2048}={}){
  const fields=new Map();let bytes=0;
  for(const event of events){
    const content=event.content||{},field=typeof content.field==='string'?event.kind+':'+content.field:event.kind+':event:'+event.sequence;
    let item=fields.get(field);if(!item){if(fields.size>=maxFields)throw new AppError(503,'正文整理字段超过边界','recording_index_capacity');item={kind:event.kind,field,provenance:event.provenance,first_sequence:event.sequence,last_sequence:event.sequence,metadata:Object.fromEntries(Object.entries(content).filter(([key])=>!['text','output','mode','field'].includes(key))),text:''};fields.set(field,item);}
    const payload=content.text??content.output,text=typeof payload==='string'?payload:payload===undefined?JSON.stringify(content):JSON.stringify(payload),previousBytes=Buffer.byteLength(item.text);
    item.text=content.mode==='replace'?text:item.text+text;item.last_sequence=event.sequence;item.metadata={...item.metadata,...Object.fromEntries(Object.entries(content).filter(([key])=>!['text','output','mode','field'].includes(key)))};
    bytes+=Buffer.byteLength(item.text)-previousBytes;if(bytes>maxBytes)throw new AppError(503,'正文整理超过边界','recording_index_capacity');
  }
  return [...fields.values()];
}
export function recordingParts(items,{partChars=2048,overlap=127}={}){
  const parts=[];
  for(const item of items){const chars=Array.from(item.text),normalized=Array.from(normalizeSearch(item.text)),length=Math.max(chars.length,normalized.length);for(let offset=0;offset<length||offset===0;offset+=partChars){const text=chars.slice(offset,offset+partChars).join(''),searchText=normalized.slice(Math.max(0,offset-overlap),offset+partChars).join('');parts.push({...item,text,offset,continuation:offset>0,searchText});}}
  return parts;
}
