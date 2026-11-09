import {RecordRedactor,redacted,sensitiveRecordField,redactRecordText} from './record-redaction.mjs';

// Tool arguments are JSON, not header lines. Decode each string before filtering
// its contents, then encode safe fragments back into that same JSON string.
export class JsonRecordRedactor{
  constructor({maxDepth=24,onTruncate=()=>{},onRedact=()=>{}}={}){if(!Number.isInteger(maxDepth)||maxDepth<1||maxDepth>128||typeof onTruncate!=='function')throw TypeError('Invalid recording JSON configuration');this.onRedact=onRedact;this.maxDepth=maxDepth;this.onTruncate=onTruncate;this.truncated=false;this.stack=[];this.string=null;this.skip=null;this.output=[];this.chars=[];this.units=0;this.closed=false;}
  flushOutput(){if(this.chars.length){this.output.push(this.chars.join(''));this.chars=[];this.units=0;}}
  emit(text){if(!text)return;this.chars.push(text);this.units+=text.length;if(this.units>=4096)this.flushOutput();}
  parentDone(){const parent=this.stack.at(-1);if(parent)parent.expect='comma';}
  flushString(){const value=this.string;if(!value||value.key||!value.decoded.length)return;const text=value.filter.push(value.decoded.join(''));value.decoded=[];if(text)this.emit(JSON.stringify(text).slice(1,-1));}
  decoded(c){
    const value=this.string;
    if(value.key){if(value.text.length<256)value.text+=c;else{value.oversized=true;this.truncated=true;this.onTruncate();}}
    else{value.decoded.push(c);if(value.decoded.length>=4096)this.flushString();}
  }
  consumeString(c){
    const value=this.string;
    if(value.unicode!==null){
      value.unicode+=c;
      if(value.unicode.length===4){this.decoded(/^[0-9a-f]{4}$/i.test(value.unicode)?String.fromCharCode(parseInt(value.unicode,16)):'\ufffd');value.unicode=null;}
      return;
    }
    if(value.escape){
      value.escape=false;if(c==='u'){value.unicode='';return;}
      this.decoded(({n:'\n',r:'\r',t:'\t',b:'\b',f:'\f'})[c]??c);return;
    }
    if(c==='\\'){value.escape=true;return;}
    if(c!=='"'){this.decoded(c);return;}
    if(value.key){
      const parent=this.stack.at(-1);if(parent){parent.sensitive=value.oversized||sensitiveRecordField(value.text);parent.expect='colon';}
      this.emit(JSON.stringify(value.oversized?'[字段名已截断]':redactRecordText(value.text,{onRedact:this.onRedact})));
    }else{
      this.flushString();const tail=value.filter.finish();if(tail)this.emit(JSON.stringify(tail).slice(1,-1));this.emit('"');this.parentDone();
    }
    this.string=null;
  }
  consumeSkip(c){
    const skip=this.skip;
    if(skip.kind==='primitive'){
      if(!/[,\]}\s]/.test(c))return true;this.skip=null;return false;
    }
    if(skip.quote){if(skip.escape)skip.escape=false;else if(c==='\\')skip.escape=true;else if(c==='"')skip.quote=false;}
    else if(c==='"')skip.quote=true;else if(c==='{'||c==='[')skip.depth++;else if(c==='}'||c===']')skip.depth--;
    if(skip.kind==='string'&&!skip.quote||skip.kind==='container'&&skip.depth===0)this.skip=null;
    return true;
  }
  push(text){
    if(this.closed)throw Error('Recording JSON filter already closed');
    if(typeof text!=='string')throw TypeError('Recording JSON text must be a string');
    for(const c of text){
      if(this.truncated)break;
      if(this.skip&&this.consumeSkip(c))continue;
      if(this.string){this.consumeString(c);continue;}
      if(/\s/.test(c)){this.emit(c);continue;}
      const parent=this.stack.at(-1);
      if(parent?.expect==='value'&&parent.sensitive){
        parent.sensitive=false;this.onRedact('sensitive_field');this.emit(JSON.stringify(redacted));this.parentDone();
        this.skip=c==='"'?{kind:'string',quote:true,escape:false,depth:0}:c==='{'||c==='['?{kind:'container',quote:false,escape:false,depth:1}:{kind:'primitive',depth:0};continue;
      }
      if(c==='"'){
        const key=parent?.kind==='object'&&parent.expect==='key';
        this.string={key,text:'',oversized:false,filter:key?null:new RecordRedactor({onRedact:this.onRedact}),decoded:[],escape:false,unicode:null};if(!key)this.emit('"');continue;
      }
      if(c==='{'||c==='['){
        if(this.stack.length>=this.maxDepth){this.truncated=true;this.onTruncate();break;}
        this.parentDone();this.stack.push({kind:c==='{'?'object':'array',expect:c==='{'?'key':'value',sensitive:false});this.emit(c);continue;
      }
      if(c==='}'||c===']'){this.stack.pop();this.emit(c);this.parentDone();continue;}
      if(c===','){if(parent)parent.expect=parent.kind==='object'?'key':'value';this.emit(c);continue;}
      if(c===':'){if(parent)parent.expect='value';this.emit(c);continue;}
      this.emit(c);this.parentDone();
    }
    this.flushString();this.flushOutput();const output=this.output.join('');this.output=[];return output;
  }
  finish(){
    if(this.closed)return '';
    if(this.string&&!this.string.key){this.flushString();const text=this.string.filter.finish();if(text)this.emit(JSON.stringify(text).slice(1,-1));}
    this.closed=true;this.stack=[];this.string=null;this.skip=null;this.flushOutput();const output=this.output.join('');this.output=[];return output;
  }
}
