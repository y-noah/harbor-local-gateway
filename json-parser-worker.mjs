import {parentPort} from 'node:worker_threads';
import {flattenJson} from './json-graph.mjs';

// No configuration, credentials, logging or upstream calls in the parser lane.
parentPort.on('message',({id,buffer})=>{
  try{
    const value=JSON.parse(Buffer.from(buffer).toString('utf8'));
    if(!value||typeof value!=='object'||Array.isArray(value))throw new SyntaxError();
    parentPort.postMessage({id,graph:flattenJson(value)});
  }catch(error){parentPort.postMessage({id,error:error instanceof SyntaxError?'invalid_json':'json_parser_failed'});}
});
