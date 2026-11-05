import {writeHeapSnapshot} from 'node:v8';
import {readFile,rm,mkdtemp} from 'node:fs/promises';
import {join} from 'node:path';
import {tmpdir} from 'node:os';
import {RecordRedactor} from '../record-redaction.mjs';

let emitted=0;globalThis.retainedRedactor=new RecordRedactor({onEmit:text=>{emitted+=text.length;if(emitted>=5000)throw Error('synthetic observer failure');}});
try{globalThis.retainedRedactor.push('visible '.repeat(128*1024));}catch(error){if(error.message!=='synthetic observer failure')throw error;}
global.gc();const directory=await mkdtemp(join(tmpdir(),'harbor-redaction-window-heap-'));let snapshot;
try{const path=writeHeapSnapshot(join(directory,'private.heapsnapshot'));snapshot=JSON.parse(await readFile(path,'utf8'));}finally{await rm(directory,{recursive:true,force:true});}
const {nodes,edges,strings}=snapshot,fields=snapshot.snapshot.meta.node_fields,edgeFields=snapshot.snapshot.meta.edge_fields,width=fields.length,edgeWidth=edgeFields.length,type=fields.indexOf('type'),name=fields.indexOf('name'),size=fields.indexOf('self_size'),edgeCount=fields.indexOf('edge_count'),edgeName=edgeFields.indexOf('name_or_index'),to=edgeFields.indexOf('to_node'),types=snapshot.snapshot.meta.node_types[type];
let edge=0,recent;
for(let node=0;node<nodes.length;node+=width){
  const count=nodes[node+edgeCount];if(types[nodes[node+type]]==='object'&&strings[nodes[node+name]]==='RecordRedactor')for(let at=edge;at<edge+count*edgeWidth;at+=edgeWidth)if(strings[edges[at+edgeName]]==='recentValue')recent=edges[at+to];
  edge+=count*edgeWidth;
}
if(recent===undefined)throw Error('redactor_window_heap_node_missing');
process.stdout.write(JSON.stringify({type:types[nodes[recent+type]],bytes:nodes[recent+size],windowUnits:globalThis.retainedRedactor.recent.length}));
