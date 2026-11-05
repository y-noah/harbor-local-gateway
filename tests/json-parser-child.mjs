import {parentPort,workerData} from 'node:worker_threads';
import {flattenJson} from '../json-graph.mjs';
parentPort.on('message',({id,buffer})=>{
 const value=JSON.parse(Buffer.from(buffer).toString('utf8'));
 if(workerData.gate&&(!workerData.gateName||value.name===workerData.gateName)){Atomics.store(workerData.gate,0,1);Atomics.notify(workerData.gate,0);Atomics.wait(workerData.gate,0,1);}
 if(workerData.exit)process.exit(7);
 parentPort.postMessage({id,graph:flattenJson(workerData.environment?{empty:Object.keys(process.env).length===0}:value)});
});
