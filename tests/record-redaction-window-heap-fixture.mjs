import {writeHeapSnapshot} from 'node:v8';
import {readFile,rm,mkdtemp} from 'node:fs/promises';
import {join} from 'node:path';
import {tmpdir} from 'node:os';
import {RecordRedactor} from '../record-redaction.mjs';

const plainWord=process.argv.includes('--plain-word');
let emitted=0;globalThis.retainedRedactor=new RecordRedactor({onEmit:text=>{emitted+=text.length;if(!plainWord&&emitted>=5000)throw Error('synthetic observer failure');}});
try{globalThis.retainedRedactor.push(plainWord?'BCDEF0123456789+/.'.repeat(65536):'visible '.repeat(128*1024));}catch(error){if(error.message!=='synthetic observer failure')throw error;}
global.gc();const directory=await mkdtemp(join(tmpdir(),'harbor-redaction-window-heap-'));let snapshot;
try{const path=writeHeapSnapshot(join(directory,'private.heapsnapshot'));snapshot=JSON.parse(await readFile(path,'utf8'));}finally{await rm(directory,{recursive:true,force:true});}
const {nodes,edges,strings}=snapshot,fields=snapshot.snapshot.meta.node_fields,edgeFields=snapshot.snapshot.meta.edge_fields,width=fields.length,edgeWidth=edgeFields.length,type=fields.indexOf('type'),name=fields.indexOf('name'),size=fields.indexOf('self_size'),edgeCount=fields.indexOf('edge_count'),edgeName=edgeFields.indexOf('name_or_index'),to=edgeFields.indexOf('to_node'),types=snapshot.snapshot.meta.node_types[type];
let edge=0,recent,redactor;const offsets=new Map();
for(let node=0;node<nodes.length;node+=width){
  offsets.set(node,edge);const count=nodes[node+edgeCount];if(types[nodes[node+type]]==='object'&&strings[nodes[node+name]]==='RecordRedactor'){redactor=node;for(let at=edge;at<edge+count*edgeWidth;at+=edgeWidth)if(strings[edges[at+edgeName]]==='recentValue')recent=edges[at+to];}
  edge+=count*edgeWidth;
}
if(recent===undefined)throw Error('redactor_window_heap_node_missing');
const result={type:types[nodes[recent+type]],bytes:nodes[recent+size],windowUnits:globalThis.retainedRedactor.recent.length};
if(plainWord){
  const edgesOf=node=>Array.from({length:nodes[node+edgeCount]},(_,i)=>offsets.get(node)+i*edgeWidth),property=(node,key)=>edgesOf(node).find(at=>strings[edges[at+edgeName]]===key),target=(node,key)=>{const at=property(node,key);if(at===undefined)throw Error('redactor_word_heap_property_missing');return edges[at+to];};
  const roots=[target(redactor,'word')],parts=target(redactor,'wordParts'),edgeType=edgeFields.indexOf('type'),edgeTypes=snapshot.snapshot.meta.edge_types[edgeType];
  for(const at of edgesOf(parts))if(edgeTypes[edges[at+edgeType]]==='element')roots.push(target(edges[at+to],'text'));
  if(roots.length<2)throw Error('redactor_word_heap_parts_missing');
  const seen=new Set(),pending=[...roots];let maximum=0;
  while(pending.length){const node=pending.pop();if(seen.has(node))continue;seen.add(node);if(seen.size>10000)throw Error('redactor_word_heap_graph_budget');const kind=types[nodes[node+type]];if(!['string','sliced string','concatenated string'].includes(kind))continue;maximum=Math.max(maximum,nodes[node+size]);for(const at of edgesOf(node))pending.push(edges[at+to]);}
  Object.assign(result,{wordUnits:globalThis.retainedRedactor.word.length,wordPartCount:roots.length-1,maximumWordBackingBytes:maximum});
}
process.stdout.write(JSON.stringify(result));
