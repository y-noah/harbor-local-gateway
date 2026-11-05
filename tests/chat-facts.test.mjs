import test from 'node:test';
import assert from 'node:assert/strict';
import {canonicalChatFacts,chatSettlementFacts,validateLegacyChat,digestChatMessages} from '../chat-facts.mjs';

test('legacy Chat validation preserves plain-text protocol and never mutates the caller history',()=>{
 const source={messages:[{role:'user',content:'synthetic',other:'discard'}],stream:false};assert.deepEqual(validateLegacyChat(source),{model:'codex',messages:[{role:'user',content:'synthetic'}],stream:false,session_id:undefined});assert.equal(source.messages[0].other,'discard');
 for(const value of [{messages:[]},{messages:[{role:'tool',content:'x'}]},{messages:[{role:'user',content:1}]},{...source,tools:[]},{...source,model:'gpt-6.1-sol'},{...source,max_tokens:1},{...source,stream:1}])assert.throws(()=>validateLegacyChat(value),{status:400});
});

test('critical Chat facts keep only typed metadata and reject credential-shaped or inconsistent resume identities',()=>{
 const facts={accountEpoch:0,dispatched:true,upstreamId:'synthetic-thread',cursor:{messageCount:2,historyDigest:'a'.repeat(64),cumulative:{input:10,output:5,cached:1},prompt:'never-copy'},text:'never-copy'};const value=canonicalChatFacts(facts);assert.equal(value.cursor.prompt,undefined);assert.equal(value.text,undefined);assert.equal(value.upstreamId,'synthetic-thread');assert.equal(canonicalChatFacts({...facts,upstreamId:'https://example.com/secret'}).upstreamId,null);assert.equal(canonicalChatFacts({...facts,upstreamId:'sk-'+('A'.repeat(60))}).upstreamId,null);assert.equal(canonicalChatFacts({...facts,cursor:{...facts.cursor,cumulative:{input:1,output:0,cached:2}}}).cursor,null);assert.throws(()=>canonicalChatFacts({...facts,accountEpoch:-1}),{code:'chat_recovery_invalid'});
});

test('Chat cursor derives from only the admitted delta and actual cumulative counters',()=>{
 const admission={account:{route_epoch:3},session:{upstream_id:null,message_count:0,history_digest:'',cum_input:0,cum_output:0,cum_cached:0},delta:[{role:'user',content:'synthetic'}]},result={text:'reply',threadId:'synthetic-thread',input:10,output:5,cached:1,usageKnown:true};const value=chatSettlementFacts(admission,result);assert.equal(value.accountEpoch,3);assert.equal(value.cursor.historyDigest,digestChatMessages('',[...admission.delta,{role:'assistant',content:'reply'}]));assert.equal(value.cursor.messageCount,2);assert.equal(chatSettlementFacts(admission,{...result,cumulativeUsage:{input:20,output:5,cached:1}}).cursor,null);assert.equal(chatSettlementFacts(admission,{...result,usageKnown:false}).cursor,null);assert.equal(chatSettlementFacts(admission,{...result,status:'failed'}).cursor,null);
});
