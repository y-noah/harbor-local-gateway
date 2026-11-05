import {performance} from 'node:perf_hooks';

// JSON containers are numbered; edges contain only primitive keys/values and
// numeric references. Structured cloning never traverses the original depth.
export function flattenJson(value){
  const containers=[value],types=[],edges=[];
  for(let parent=0;parent<containers.length;parent++){
    const node=containers[parent];types.push(Array.isArray(node)?1:0);
    for(const key of Object.keys(node)){const child=node[key];if(child!==null&&typeof child==='object'){const ref=containers.length;containers.push(child);edges.push(parent,key,1,ref);}else edges.push(parent,key,0,child);}
  }
  return {types,edges};
}
export async function rebuildJson(graph,{sliceMs=5,cancelled=()=>false}={}){
  if(!graph||!Array.isArray(graph.types)||!Array.isArray(graph.edges)||graph.types[0]!==0||graph.edges.length%4)throw Error('Invalid JSON graph');
  const nodes=new Array(graph.types.length),seen=new Uint8Array(nodes.length);seen[0]=1;let deadline=performance.now()+sliceMs;
  const checkpoint=async()=>{await new Promise(setImmediate);if(cancelled())throw Error('JSON reconstruction cancelled');deadline=performance.now()+sliceMs;};
  for(let index=0;index<nodes.length;index++){if(cancelled())throw Error('JSON reconstruction cancelled');const type=graph.types[index];if(type!==0&&type!==1)throw Error('Invalid JSON container');nodes[index]=type===1?[]:{};if(performance.now()>=deadline)await checkpoint();}
  for(let at=0;at<graph.edges.length;at+=4){
    if(cancelled())throw Error('JSON reconstruction cancelled');const parent=graph.edges[at],key=graph.edges[at+1],reference=graph.edges[at+2],value=graph.edges[at+3];
    if(!Number.isSafeInteger(parent)||parent<0||parent>=nodes.length||typeof key!=='string'||reference!==0&&reference!==1)throw Error('Invalid JSON edge');
    if(reference){if(!Number.isSafeInteger(value)||value<=parent||value>=nodes.length||seen[value])throw Error('Invalid JSON reference');seen[value]=1;}
    else if(value!==null&&!['string','number','boolean'].includes(typeof value))throw Error('Invalid JSON primitive');
    // Assignment would invoke Object.prototype.__proto__; JSON.parse creates
    // own data properties even for prototype-sensitive names.
    Object.defineProperty(nodes[parent],key,{value:reference?nodes[value]:value,enumerable:true,writable:true,configurable:true});
    if(performance.now()>=deadline)await checkpoint();
  }
  for(const value of seen){if(!value)throw Error('Unreachable JSON node');if(cancelled())throw Error('JSON reconstruction cancelled');if(performance.now()>=deadline)await checkpoint();}return nodes[0];
}
