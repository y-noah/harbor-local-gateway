export const redacted='[已脱敏]';
export const recordRedactionCategories=Object.freeze(['credential','sensitive_field','private_key','attachment_data']);
export function recordRedactionSummary(categories){return {version:1,policy:'credentials-v1',categories:[...categories].sort()};}
export function validRecordRedaction(value){return !!value&&typeof value==='object'&&!Array.isArray(value)&&Object.keys(value).length===3&&Object.keys(value).every(key=>['version','policy','categories'].includes(key))&&value.version===1&&value.policy==='credentials-v1'&&Array.isArray(value.categories)&&value.categories.length<=recordRedactionCategories.length&&new Set(value.categories).size===value.categories.length&&value.categories.every(category=>recordRedactionCategories.includes(category));}
const safeRecords=new WeakSet();
// This provenance lives only in process memory; no request-supplied flag can
// opt out of filtering. Authenticated decryption is the only restore boundary.
export function restoreAuthenticatedRecord(value){
  if(value&&typeof value==='object'){
    for(const item of Object.values(value))restoreAuthenticatedRecord(item);
    Object.freeze(value);safeRecords.add(value);
  }return value;
}
export function filteredRecordField(metadata,field,value){
  if(!['text','output'].includes(field))throw TypeError('Invalid filtered recording field');
  if(field==='text'&&typeof value!=='string'||value&&typeof value==='object'&&!safeRecords.has(value))throw TypeError('Recording field has no filtered provenance');
  const safe={...sanitizeRecord(metadata),[field]:value};return restoreAuthenticatedRecord(safe);
}
export function sensitiveRecordField(key){
  const name=String(key).replace(/\\u([0-9a-f]{4})/gi,(_,hex)=>String.fromCharCode(parseInt(hex,16))).replace(/[^a-z0-9]/gi,'').toLowerCase();
  return ['key','auth','authorization','proxyauthorization','cookie','setcookie','credential','credentials','b64json','base64','encryptedcontent'].includes(name)||/(?:password|passwd|secret|token|apikey|privatekey|signature|credential|accesskeyid|pat)$/.test(name);
}
// Keep hyphenated credential detection conservative; the underscore form needs
// a lexical boundary so ordinary identifiers such as task_result stay intact.
const credentialWord=/sk-|[/.\\-]sk_|hg_|github_pat_|gh[pousr]_|xox[baprs]-|AIza|AKIA|eyJ/;
const wordCharacter=/[a-zA-Z0-9_+/.\\-]/;
const quoteCharacter=c=>c==='"'||c==="'";
// A credential match can first appear only when its final literal arrives.
// Derive those literals from the unchanged rules. Unsupported regex syntax
// disables the optimization, retaining the ordinary character state machine.
function credentialTerminals(){
  const ends=new Set();
  for(const alternative of credentialWord.source.split('|')){
    let inClass=false,escaped=false,last=null;
    for(const c of alternative){
      if(inClass){if(escaped)escaped=false;else if(c==='\\')escaped=true;else if(c===']')inClass=false;continue;}
      if(c==='['){inClass=true;last=null;continue;}
      if(!/[A-Za-z0-9_-]/.test(c))return null;last=c;
    }
    if(inClass||!last)return null;ends.add(last.charCodeAt(0));
  }
  return ends;
}
const credentialTerminalCodes=credentialTerminals();
const plainWordCode=code=>credentialTerminalCodes&&!credentialTerminalCodes.has(code)&&((code>=48&&code<=57)||(code>=65&&code<=90)||(code>=97&&code<=122)||code===43||code===46||code===47);

// Raw text is retained only inside this bounded filter, before any recording queue or file.
export class RecordRedactor{
  constructor({onEmit,onRedact=()=>{}}={}){this.onEmit=onEmit;this.onRedact=onRedact;this.source=null;this.wordParts=[];this.word='';this.wordHead='';this.wordTail='';this.wordOffset=0;this.hiddenWord=false;this.output=[];this.outputChars=[];this.outputUnits=0;this.recent='';this.pendingKey=false;this.pendingHeader=false;this.quotedKey=false;this.valuePending=false;this.slashes=0;this.quoted=null;this.unquoted=null;this.jsonSkip=null;this.enclosingQuote=null;this.keyQuote=false;this.closedKey=false;this.jsonValuePending=false;this.unquotedQuote=null;this.quoteEscape=0;this.privateBlock=false;this.binary=false;this.high='';this.closed=false;}
  // Most characters never inspect the PEM/data-URI lookbehind. Retain the
  // current chunk and its UTF-16 cursor rather than copying 100 units per
  // character. The accessor also preserves the exact view inside onEmit.
  get recent(){
    if(this.recentText==null)return this.recentValue;
    const end=this.recentPosition,start=Math.max(this.recentStart,end-100),prefix=end-this.recentStart<100?this.recentBase:'';
    return (prefix+this.recentText.slice(start,end)).slice(-100);
  }
  set recent(value){
    if(this.recentText==null)this.recentValue=value;
    else{this.recentBase=value;this.recentStart=this.recentPosition;}
  }
  flushOutput(){if(this.outputChars.length){this.output.push(this.outputChars.join(''));this.outputChars=[];this.outputUnits=0;}}
  emit(text,source=this.source){if(!text)return;this.onEmit?.(text,source);this.outputChars.push(text);this.outputUnits+=text.length;if(this.outputUnits>=4096)this.flushOutput();}
  wordPrefix(count){
    while(count>0&&this.wordParts.length){const part=this.wordParts[0],take=Math.min(count,part.text.length);this.emit(part.text.slice(0,take),part.source);count-=take;if(take===part.text.length)this.wordParts.shift();else part.text=part.text.slice(take);}
  }
  flushWord(){
    if(!this.word&&!this.hiddenWord)return;
    const word=this.word;this.word='';this.wordHead='';this.wordTail='';this.wordOffset=0;
    if(this.hiddenWord){this.hiddenWord=false;this.wordParts=[];this.pendingKey=false;return;}
    this.wordPrefix(word.length);this.pendingKey=sensitiveRecordField(word);this.quotedKey=false;this.pendingHeader=/^(?:authorization|proxy-authorization|cookie|set-cookie)$/i.test(word);
    if(/^bearer$/i.test(word)){this.valuePending='token';this.pendingKey=false;}
  }
  pendingValue(c,escaped=false){
    if(this.valuePending!=='json'&&c===this.enclosingQuote&&!escaped){this.emit(c);this.enclosingQuote=null;this.valuePending=false;return;}
    if(/\s/.test(c)&&!this.slashes){this.emit(c);return;}
    this.jsonValuePending=false;this.closedKey=false;
    if(this.valuePending==='header'){
      this.onRedact('sensitive_field');this.emit(redacted);this.unquoted='header';this.unquotedQuote=this.enclosingQuote;this.valuePending=false;this.slashes=0;return;
    }
    if(c==='\\'){this.slashes++;return;}
    this.onRedact(this.valuePending==='token'?'credential':'sensitive_field');
    if(quoteCharacter(c)){
      this.emit('\\'.repeat(this.slashes)+c+redacted);this.quoted={quote:c,opening:this.slashes,run:0};this.slashes=0;this.valuePending=false;return;
    }
    if(this.valuePending==='json'){
      this.emit(JSON.stringify(redacted));this.jsonSkip={primitive:c!=='{'&&c!=='[',depth:c==='{'||c==='['?1:0,quote:false,escaped:false};
    }else{this.emit(redacted);this.unquoted=this.valuePending;this.unquotedQuote=this.enclosingQuote;}
    this.valuePending=false;this.slashes=0;
  }
  trackContext(c,escaped){
    if(this.enclosingQuote){if(c==='"'&&!escaped)this.enclosingQuote=null;return;}
    if(c==='"'&&!escaped){
      if(this.keyQuote){this.keyQuote=false;this.closedKey=true;}
      else if(this.jsonValuePending){this.enclosingQuote='"';this.jsonValuePending=false;this.closedKey=false;}
      else{this.keyQuote=true;this.closedKey=false;}
      return;
    }
    if(this.keyQuote||/\s/.test(c))return;
    if(c===':'&&this.closedKey){this.jsonValuePending=true;this.closedKey=false;return;}
    this.closedKey=false;this.jsonValuePending=false;
  }
  push(text,source=null){
    if(this.closed)throw Error('Recording redactor already closed');
    if(typeof text!=='string')throw TypeError('Recording text must be a string');
    const previousHigh=this.high,previousSource=this.highSource;this.source=source;text=previousHigh+text;this.high='';this.highSource=null;
    const last=text.charCodeAt(text.length-1);if(last>=0xd800&&last<=0xdbff){this.high=text.slice(-1);this.highSource=source;text=text.slice(0,-1);}
    this.recentBase=this.recent;this.recentText=text;this.recentStart=0;this.recentPosition=0;
    let first=true;
    try{for(let position=0;position<text.length;){
      const code=text.codePointAt(position),c=String.fromCodePoint(code);position+=c.length;
      this.source=first&&previousHigh?previousSource:source;first=false;
      if(!this.jsonSkip&&!this.privateBlock&&!this.binary&&!this.quoted&&!this.unquoted&&!this.valuePending&&!this.hiddenWord&&plainWordCode(code)){
        const start=position-1,limit=Math.min(text.length,start+8193-this.word.length);let end=position;
        while(end<limit&&plainWordCode(text.charCodeAt(end)))end++;
        // Copy this bounded ASCII run, so a retained word cannot pin a whole
        // raw input chunk through V8 sliced-string backing storage.
        const run=text.slice(start,end).split('').join('');position=end;
        const escaped=this.quoteEscape%2===1;this.quoteEscape=0;this.recentPosition+=run.length;this.trackContext(c,escaped);
        this.word+=run;this.wordHead=(this.wordHead+run).slice(0,3);this.wordTail=(this.wordTail+run).slice(-16);
        const part=this.wordParts.at(-1);if(part?.source===this.source)part.text+=run;else this.wordParts.push({source:this.source,text:run});
        if(this.word.length>8192){const length=this.word.length-64;this.wordPrefix(length);this.wordOffset+=length;this.word=this.word.slice(-64);}
        continue;
      }
      const escaped=this.quoteEscape%2===1;this.quoteEscape=c==='\\'?this.quoteEscape+1:0;
      if(c==='\n'||c==='\r'){this.enclosingQuote=null;this.keyQuote=false;this.closedKey=false;this.jsonValuePending=false;}
      this.recentPosition+=c.length;
      if(this.jsonSkip){
        const skip=this.jsonSkip;
        if(skip.primitive){if(!/[,\]}\s]/.test(c))continue;this.jsonSkip=null;this.pendingKey=false;this.quotedKey=false;}
        else{
          if(skip.quote){if(skip.escaped)skip.escaped=false;else if(c==='\\')skip.escaped=true;else if(c==='"')skip.quote=false;}
          else if(c==='"')skip.quote=true;else if(c==='{'||c==='[')skip.depth++;else if(c==='}'||c===']')skip.depth--;
          if(skip.depth===0){this.jsonSkip=null;this.pendingKey=false;this.quotedKey=false;}continue;
        }
      }
      if(this.privateBlock){
        if(c==='-'&&/-----END [A-Z0-9 ]*PRIVATE KEY(?: BLOCK)?-----$/.test(this.recent)){this.privateBlock=false;this.recent='';this.pendingKey=false;}
        continue;
      }
      if(c==='-'&&/-----BEGIN [A-Z0-9 ]*PRIVATE KEY(?: BLOCK)?-----$/.test(this.recent)){
        const hidden=this.quoted||this.unquoted||this.hiddenWord;this.word='';this.wordHead='';this.wordTail='';this.wordOffset=0;this.wordParts=[];this.hiddenWord=false;this.valuePending=false;this.pendingKey=false;if(!hidden)this.emit(redacted);this.onRedact('private_key');this.privateBlock=true;continue;
      }
      if(this.binary){if(/[A-Za-z0-9+/=]/.test(c))continue;this.binary=false;}
      if(this.quoted){
        const q=this.quoted;if(c==='\\'){q.run++;continue;}
        if(c===q.quote&&q.run%(2*(q.opening+1))===q.opening){this.emit('\\'.repeat(q.opening)+c);this.quoted=null;this.pendingKey=false;}
        else q.run=0;
        continue;
      }
      if(this.unquoted){
        const stop=this.unquoted==='token'?/\s/.test(c)||quoteCharacter(c)&&!escaped:/[\r\n]/.test(c)||c===this.unquotedQuote&&!escaped;
        if(!stop)continue;this.unquoted=null;this.pendingKey=false;
      }
      if(this.valuePending){this.pendingValue(c,escaped);continue;}
      if(this.hiddenWord){if(!/[\s"']/.test(c)&&c.codePointAt(0)<128)continue;this.flushWord();}
      if(wordCharacter.test(c)){
        if(this.hiddenWord)continue;
        this.trackContext(c,escaped);
        // Keep detection windows separate from the growing retained word so
        // prefix/suffix checks do not repeatedly flatten its full string.
        this.word+=c;if(this.wordHead.length<3)this.wordHead+=c;this.wordTail=(this.wordTail+c).slice(-16);const part=this.wordParts.at(-1);if(part?.source===this.source)part.text+=c;else this.wordParts.push({source:this.source,text:c});
        if(this.wordOffset===0&&this.wordHead==='sk_'||credentialWord.test(this.wordTail)){const first=this.wordParts[0]?.source;this.word='';this.wordHead='';this.wordTail='';this.wordOffset=0;this.wordParts=[];this.hiddenWord=true;this.pendingKey=false;this.onRedact('credential');this.emit(redacted,first);}
        else if(this.word.length>8192){const length=this.word.length-64;this.wordPrefix(length);this.wordOffset+=length;this.word=this.word.slice(-64);}
        continue;
      }
      this.flushWord();
      if(this.valuePending){this.pendingValue(c,escaped);continue;}
      this.trackContext(c,escaped);
      if(this.pendingKey&&quoteCharacter(c))this.quotedKey=true;
      if(this.pendingKey&&(c===':'||c==='=')){this.valuePending=this.quotedKey&&c===':'?'json':this.pendingHeader?'header':'field';this.pendingKey=false;this.quotedKey=false;this.emit(c);continue;}
      if(this.pendingKey&&!/[\s\\"']/.test(c))this.pendingKey=false;
      this.emit(c);
      if(c===','&&/data:[^\s]{0,80};base64,$/i.test(this.recent)){this.binary=true;this.onRedact('attachment_data');this.emit('[附件数据已省略]');}
    }}finally{
      // A sliced string may retain its entire input in V8. Copy these at most
      // 100 UTF-16 units without an encoding roundtrip (lone surrogates matter).
      this.recentValue=this.recent.split('').join('');this.recentText=null;this.recentBase='';
    }
    this.flushOutput();const result=this.output.join('');this.output=[];return result;
  }
  finish(){
    if(this.closed)return '';
    let prefix='';if(this.high){this.high='';prefix=this.push('\ufffd',this.highSource);}
    this.flushWord();
    this.closed=true;this.flushOutput();const result=prefix+this.output.join('');this.output=[];this.recent='';this.word='';this.wordHead='';this.wordTail='';this.wordParts=[];this.quoted=null;this.unquoted=null;this.jsonSkip=null;this.privateBlock=false;this.slashes=0;return result;
  }
}
export function redactRecordText(text,options={}){const filter=new RecordRedactor(options);return filter.push(text)+filter.finish();}
export function boundedRecordText(text,maxBytes){
  // Inspect at most the remaining budget, never allocate/scan the full raw string.
  let prefix=text.slice(0,maxBytes);if(Buffer.byteLength(prefix)<=maxBytes)return prefix;
  let lo=0,hi=prefix.length;while(lo<hi){const mid=Math.ceil((lo+hi)/2);if(Buffer.byteLength(prefix.slice(0,mid))<=maxBytes)lo=mid;else hi=mid-1;}
  if(lo&&/[\uD800-\uDBFF]/.test(prefix[lo-1]))lo--;return prefix.slice(0,lo);
}
export function redactRecordParts(parts,{maxBytes=128*1024,maxParts=4096,onTruncate=()=>{},onRedact}={}){
  const count=Math.min(parts.length,maxParts),outputs=Array.from({length:count},()=>({chars:[],units:0,chunks:[]}));let remaining=maxBytes;
  if(count!==parts.length)onTruncate();
  const flush=out=>{if(out.chars.length){out.chunks.push(out.chars.join(''));out.chars=[];out.units=0;}};
  const filter=new RecordRedactor({onRedact,onEmit:(text,index)=>{const out=outputs[index];if(!out)return;out.chars.push(text);out.units+=text.length;if(out.units>=4096)flush(out);}});
  for(let index=0;index<count;index++){
    const raw=parts[index],prefix=boundedRecordText(raw,Math.max(0,remaining));remaining-=Buffer.byteLength(prefix);filter.push(prefix,index);
    if(prefix.length!==raw.length){onTruncate();break;}
  }
  filter.finish();return {parts:outputs.map(out=>{flush(out);return out.chunks.join('');}),bytes:maxBytes-remaining};
}
export function sanitizeRecord(value,{maxDepth=24,maxBytes=128*1024,maxNodes=4096,onTruncate=()=>{},onRedact=()=>{}}={}){
  let remaining=maxBytes,nodes=maxNodes;
  const cut=()=>{onTruncate();return '[已截断]';};
  const visit=(item,depth)=>{
    if(depth>maxDepth||--nodes<0||remaining<=0)return cut();
    if(item&&typeof item==='object'&&safeRecords.has(item)){
      const bytes=Buffer.byteLength(JSON.stringify(item));if(bytes>remaining)return cut();remaining-=bytes;return item;
    }
    if(typeof item==='string'){
      const prefix=boundedRecordText(item,remaining);remaining-=Buffer.byteLength(prefix);
      const clipped=prefix.length!==item.length;if(clipped)onTruncate();
      return redactRecordText(prefix,{onRedact})+(clipped?'[已截断]':'');
    }
    if(item instanceof ArrayBuffer||ArrayBuffer.isView(item)){onRedact('attachment_data');return {type:'binary',bytes:item.byteLength,content:'[附件数据已省略]'};}
    if(Array.isArray(item)){
      const result=[],parts=[],positions=[];
      for(const child of item){
        if(nodes<=0||remaining<=0){result.push(cut());break;}
        if(child?.type==='text'&&typeof child.text==='string'){
          nodes-=3;const prefix=boundedRecordText(child.text,remaining);remaining-=Buffer.byteLength(prefix);if(prefix.length!==child.text.length)onTruncate();
          positions.push(result.length);parts.push(prefix);result.push({type:'text',text:''});
        }else result.push(visit(child,depth+1));
      }
      const safe=redactRecordParts(parts,{maxBytes,maxParts:maxNodes,onTruncate,onRedact}).parts;for(let i=0;i<safe.length;i++)result[positions[i]].text=safe[i];return result;
    }
    if(item&&typeof item==='object'){
      if(['image','image_url','input_image','input_file','file','audio','input_audio'].includes(item.type)){onRedact('attachment_data');return {type:item.type,...(Number.isSafeInteger(item.size??item.bytes)&&(item.size??item.bytes)>=0?{bytes:item.size??item.bytes}:{}),...(Number.isSafeInteger(item.position)&&item.position>=0?{position:item.position}:{}),...(['low','high','auto'].includes(item.detail)?{detail:item.detail}:{}),content:'[附件数据已省略]'};}
      const result={};for(const key in item){if(!Object.hasOwn(item,key))continue;if(nodes<=0||remaining<=0){Object.defineProperty(result,'recording_truncated',{value:cut(),enumerable:true});break;}
        const keyPrefix=boundedRecordText(key,Math.min(remaining,256));if(keyPrefix.length!==key.length)onTruncate();
        const safeKey=redactRecordText(keyPrefix,{onRedact});remaining-=Buffer.byteLength(safeKey);
        if(keyPrefix.length===key.length&&sensitiveRecordField(keyPrefix))onRedact('sensitive_field');
        Object.defineProperty(result,safeKey,{value:keyPrefix.length!==key.length||sensitiveRecordField(keyPrefix)?redacted:visit(item[key],depth+1),enumerable:true,writable:true,configurable:true});
      }return result;
    }
    return item;
  };return restoreAuthenticatedRecord(visit(value,0));
}
export function sanitizeToolRecord(value,options={}){
  if(typeof value==='string'){
    const maximum=options.maxBytes??128*1024;
    const prefix=boundedRecordText(value,maximum);
    if(/^[\[{]/.test(prefix.trimStart())){
      if(prefix.length!==value.length){options.onTruncate?.();return '[工具结构化内容超限，已省略]';}
      try{return JSON.stringify(sanitizeRecord(JSON.parse(value),options));}catch{}
    }
  }
  return sanitizeRecord(value,options);
}
