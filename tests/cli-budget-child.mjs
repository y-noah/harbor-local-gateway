// Synthetic subprocess used by isolated PG/HTTP lifecycle tests only.
import {createInterface} from 'node:readline';
const mode=process.argv[2];
if(mode==='version'){console.log('codex-cli 0.160.1');process.exit(0);}
if(mode==='check'){console.log('ChatGPT');process.exit(0);}
if(mode==='claude-check'){console.log(JSON.stringify({loggedIn:true,authMethod:'claude.ai'}));process.exit(0);}
if(mode==='qoder-models'){console.log('MODEL\nPerformance');process.exit(0);}
if(mode==='qoder-run'){process.stdin.resume();process.stdin.once('end',()=>{console.log(JSON.stringify({result:'synthetic Qoder result',usage:{input_tokens:100,output_tokens:20,cache_read_input_tokens:30}}));process.exit(0);});}
else{
 process.on('SIGTERM',()=>{});setInterval(()=>{},1000);
 if(mode==='quota-eof-held')process.on('message',message=>{if(message==='release')process.exit(0);});
 const send=value=>console.log(JSON.stringify(value));
 createInterface({input:process.stdin}).on('line',line=>{const value=JSON.parse(line);if(value.id===1)send({id:1,result:{}});if(value.method==='account/rateLimits/read')send({id:2,result:{rateLimits:{primary:{usedPercent:25,windowDurationMins:300,resetsAt:9999999999}}}});if(value.method==='account/read'){const finish=()=>send({id:2,result:{account:{type:'chatgpt'}}});if(mode==='refresh-held')process.once('message',finish);else finish();}if(value.method==='account/login/start'){send({id:2,result:{type:'chatgptDeviceCode',verificationUrl:'https://auth.openai.com/codex/device',loginId:'synthetic-login',userCode:'ABCD-1234'}});if(mode==='login-complete')send({method:'account/login/completed',params:{loginId:'synthetic-login',success:true}});}});
}
