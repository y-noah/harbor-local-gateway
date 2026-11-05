import test from 'node:test';
import assert from 'node:assert/strict';
import {execFile} from 'node:child_process';
import {promisify} from 'node:util';
import {RecordRedactor,redacted,redactRecordText} from '../record-redaction.mjs';

test('plain runs retain every credential boundary and the original emitting source across every split',()=>{
  const prefixes=['sk-','sk_','/sk_','.sk_','\\sk_','-sk_','hg_','github_pat_','ghp_','gho_','ghu_','ghs_','ghr_','xoxb-','xoxa-','xoxp-','xoxr-','xoxs-','AIza','AKIA','eyJ'];
  for(const prefix of prefixes){
    const plain='Bcef012+/. ',text=plain+prefix+'synthetic-private visible',expected=plain+redacted+' visible';
    for(let split=0;split<=text.length;split++){
      const emits=[],filter=new RecordRedactor({onEmit:(value,source)=>emits.push({value,source})});
      const output=filter.push(text.slice(0,split),0)+filter.push(text.slice(split),1)+filter.finish();
      assert.equal(output,expected,prefix+':'+split);assert.equal(emits.map(e=>e.value).join(''),expected);
      assert.equal(emits.find(e=>e.value===redacted)?.source,split>plain.length?0:1);
    }
  }
});

test('long ordinary words preserve text, quote state and Unicode at the bounded word edges',()=>{
  const text='Bcef012+/.'.repeat(1100)+' 中🍋\ud800 visible';
  for(const width of [1,2,17,8191,8192,8193,text.length]){
    const filter=new RecordRedactor(),parts=[];
    for(let at=0;at<text.length;at+=width)parts.push(filter.push(text.slice(at,at+width)));
    parts.push(filter.finish());assert.equal(parts.join(''),text);
  }
  const visible='Bcef012+/.'.repeat(1000),input={password:visible,visible};
  assert.deepEqual(JSON.parse(redactRecordText(JSON.stringify(input))),{password:redacted,visible});
});

test('plain runs retain only bounded independent word backing in the actual V8 heap',{timeout:30000},async()=>{
  const {stdout}=await promisify(execFile)(process.execPath,['--expose-gc',new URL('./record-redaction-window-heap-fixture.mjs',import.meta.url).pathname,'--plain-word'],{timeout:20000,maxBuffer:4096});
  const result=JSON.parse(stdout);assert.ok(result.wordUnits<=8192);assert.ok(result.wordPartCount>0);assert.ok(result.maximumWordBackingBytes<=2*8193+128);assert.ok(result.bytes<=512);
});
