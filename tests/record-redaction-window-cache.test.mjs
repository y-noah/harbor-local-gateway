import test from 'node:test';
import assert from 'node:assert/strict';
import {createHash} from 'node:crypto';
import {RecordRedactor,redacted} from '../record-redaction.mjs';

// Finite output and onEmit/source goldens from trusted pre-change Git blob
// 2e69a2d182a0ec096aa29b5bd427d0e704128df0 (SHA256
// 498450da9fdf3c12e860b928b6e12f3ead9fabb62b5235a62686c6eb1eda6140).
// CI needs neither Git nor another copy of the production state machine.
const vectors=[
 {name:'split-prefix-and-new-word',chunks:['task_result s','k_','synthetic-private ','plain']},
 {name:'clipped-word-credential-and-surrogate',chunks:['a'.repeat(8191)+'/','s','k_','synthetic-private ','\ud83c','\udf4b',' visible']},
 {name:'interior-underscore-after-clipping-stays-visible',chunks:['a'.repeat(8191),'a'.repeat(9),'sk_','synthetic visible']},
 {name:'private-block-to-json-to-next-word',chunks:['-----BEGIN RSA PRI','VATE KEY-----\nsynthetic\n-----EN','D RSA PRIVATE KEY----- normal ','{"api_','key":"synthetic","path":"visible"}',' sk_synthetic done']},
 {name:'source-and-unicode-across-hidden-transitions',chunks:['正常\ud83c','\udf4b hg_','synthetic-private\n','x'.repeat(8193),'github_pat_','synthetic-private ','visible']}
];

const goldens=[
  {
    "name": "split-prefix-and-new-word",
    "outputHash": "4b8eeb041de91de0e5c547a451223f4e74759110324762170813cec8bba573c4",
    "emitsHash": "3181d9645169ee8626f20f20845457aa9fa81a04d027a242b01558602425bffe",
    "outputLength": 23,
    "emitCount": 5,
    "short": "task_result [已脱敏] plain"
  },
  {
    "name": "clipped-word-credential-and-surrogate",
    "outputHash": "b6ed243e218d9b0b12e4d67604a048b4dc6d48ad6b560752e098a1af6ee92add",
    "emitsHash": "4cb4224f9afa6780518e241add9c1e004e749e99c95330b01d91f0c926050c45",
    "outputLength": 8145,
    "emitCount": 6,
    "short": null
  },
  {
    "name": "interior-underscore-after-clipping-stays-visible",
    "outputHash": "276597d8c0694eb46683dbf45988fe1d0be4d751fd9bdb7eebaeab531c1ec8ea",
    "emitsHash": "25780699da6a9bf4722de378477964b89d77bf4e0510530a8271fdd14b7d14fa",
    "outputLength": 8220,
    "emitCount": 7,
    "short": null
  },
  {
    "name": "private-block-to-json-to-next-word",
    "outputHash": "698d638fb15826418261dd9b0b51ec4f4ec063e589de1307ba88e2aa588d032c",
    "emitsHash": "18db1ad40356664754da301b07a6825fe9eb62c1e10f98f9f0e597a18fbae21a",
    "outputLength": 83,
    "emitCount": 32,
    "short": "-----BEGIN RSA PRIVATE [已脱敏] normal {\"api_key\":\"[已脱敏]\",\"path\":\"visible\"} [已脱敏] done"
  },
  {
    "name": "source-and-unicode-across-hidden-transitions",
    "outputHash": "e60ec7b5171ba1e0f9c3f833ca4a22e256fc8aa4b862dcbf204c1c11ea70b0ba",
    "emitsHash": "19a66bb0fa39b87f4d057c0465b27ca70af4f56cbe30867cba476b343fa593f2",
    "outputLength": 8153,
    "emitCount": 10,
    "short": null
  }
];

const sha=text=>createHash('sha256').update(text).digest('hex');
for(const vector of vectors)test('trusted source-preserving redaction: '+vector.name,()=>{
 const expected=goldens.find(value=>value.name===vector.name),sources=vector.chunks.map((_,id)=>Object.freeze({id})),emits=[],filter=new RecordRedactor({onEmit:(text,source)=>{const id=sources.indexOf(source);assert.ok(id>=0,'source identity must be an original caller object');emits.push([text,id]);}});let output='';
 for(let index=0;index<vector.chunks.length;index++){output+=filter.push(vector.chunks[index],sources[index]);assert.ok(filter.wordHead.length<=3);assert.ok(filter.wordTail.length<=16);assert.ok(filter.word.length<=8192);assert.ok(filter.recent.length<=100);}output+=filter.finish();
 assert.equal(sha(output),expected.outputHash);assert.equal(sha(JSON.stringify(emits)),expected.emitsHash);assert.equal(output.length,expected.outputLength);assert.equal(emits.length,expected.emitCount);if(expected.short!==null)assert.equal(output,expected.short);assert.equal(filter.wordHead,'');assert.equal(filter.wordTail,'');
});

test('all credential patterns remain hidden across chunk boundaries and the next ordinary word remains visible',()=>{
 const prefixes=['sk-','sk_','/sk_','foo-sk_','hg_','github_pat_','ghp_','gho_','ghu_','ghs_','ghr_','xoxb-','xoxp-','xoxa-','xoxr-','xoxs-','AIza','AKIA','eyJ'];
 for(const prefix of prefixes)for(const width of [1,2,3,7,15,16,17]){const raw=prefix+'synthetic-private visible',filter=new RecordRedactor();let output='';for(let offset=0;offset<raw.length;offset+=width){output+=filter.push(raw.slice(offset,offset+width));assert.ok(filter.wordHead.length<=3);assert.ok(filter.wordTail.length<=16);}output+=filter.finish();assert.equal(output,redacted+' visible',prefix+' width='+width);assert.equal(filter.wordHead,'');assert.equal(filter.wordTail,'');}
});

test('clipping beyond 8192 retains suffix detection without treating an interior underscore prefix as a new word',()=>{
 for(const width of [1,7,16,63,8191,8192,8193])for(const hidden of [true,false]){const raw='a'.repeat(8200)+(hidden?'/sk_synthetic':'sk_synthetic')+' visible',filter=new RecordRedactor();let output='';for(let offset=0;offset<raw.length;offset+=width)output+=filter.push(raw.slice(offset,offset+width));output+=filter.finish();assert.equal(output,hidden?'a'.repeat(8129)+redacted+' visible':raw);}
});
