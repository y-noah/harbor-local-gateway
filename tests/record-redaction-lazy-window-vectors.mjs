import {createHash} from 'node:crypto';

export function lazyWindowVectors(){
  let state=0x34be781a;const next=()=>{state^=state<<13;state^=state>>>17;state^=state<<5;return state>>>0;};
  const split=(text,width)=>{const chunks=[];for(let index=0;index<text.length;){const end=Math.min(text.length,index+1+next()%width);chunks.push(text.slice(index,end));index=end;}return chunks;};
  const blocks=['plain text ','正常🍋文字 ','a'.repeat(97),'-----BEGIN RSA PRIVATE KEY-----\nsynthetic\n-----END RSA PRIVATE KEY----- ','data:image/png;base64,QUJDREVGRw== done ',String.raw`{"api_key":"synthetic","safe":"visible"} `,'Bearer synthetic-value\r\nvisible ',String.raw`"password" : {"nested":"synthetic"},"plain":true `,'hg_synthetic-private sk_synthetic-private github_pat_synthetic-private ',String.raw`\\\"token\\\":\\\"synthetic\\\" `];
  const cases=[];
  for(let index=0;index<96;index++){
    let text='';for(let part=0;part<8+next()%16;part++)text+=blocks[next()%blocks.length];
    cases.push({name:'seeded-'+index,chunks:split(text,1+next()%141)});
  }
  const adversarial=[
    '🍋'.repeat(50)+'-----BEGIN OPENSSH PRIVATE KEY-----\nsynthetic\n-----END OPENSSH PRIVATE KEY----- done',
    'x'.repeat(101)+'data:application/octet-stream;base64,QUJDREVGRw== visible',
    'a'.repeat(256*1024)+'/sk_synthetic-private visible',
    '-'.repeat(128*1024)+'-----BEGIN RSA PRIVATE KEY-----\nsynthetic\n-----END RSA PRIVATE KEY----- visible',
    '{"secret":'+JSON.stringify('x'.repeat(128*1024))+',"visible":true}',
    '-----BEGIN RSA PRIVATE KEY-----\n'+('x'.repeat(128*1024))+'\n-----END RSA PRIVATE KEY-----data:text/plain;base64,QUJD done',
    'x'.repeat(99)+'\ud83c'+'\udf4b'+'-----BEGIN PRIVATE KEY-----synthetic-----END PRIVATE KEY----- done'
  ];
  for(const [index,text] of adversarial.entries())for(const width of [17,4096])cases.push({name:'adversarial-'+index+'-'+width,chunks:split(text,width)});
  return cases;
}

// Include the observer-visible UTF-16 lookbehind as well as every emitted
// byte/source, returned chunk and final state. No raw fixture is persisted.
export function lazyWindowDigests(Redactor){
  const output=createHash('sha256'),emits=createHash('sha256'),windows=createHash('sha256');let count=0;
  for(const vector of lazyWindowVectors()){
    let filter,ordinal=0;filter=new Redactor({onEmit:(text,source)=>{emits.update(JSON.stringify([vector.name,text,source]));if(++ordinal%29===0)windows.update(JSON.stringify(filter.recent));}});
    const chunks=vector.chunks.map((chunk,index)=>{const value=filter.push(chunk,index%3);windows.update(JSON.stringify(filter.recent));return value;});chunks.push(filter.finish());
    output.update(JSON.stringify([vector.name,chunks]));windows.update(JSON.stringify(filter.recent));count++;
  }
  return {count,output:output.digest('hex'),emits:emits.digest('hex'),windows:windows.digest('hex')};
}
