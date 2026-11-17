import test from 'node:test';
import assert from 'node:assert/strict';
import {requestPricingDiagnostic} from '../request-pricing-diagnostic.mjs';

const fields=['input','output','cached','adjustment','total'];
const request=()=>({price_version:'fixed-v1',model:'synthetic-model',billing_model:null,started:100,cached:0});
const quote=(state='priced',amount=0n)=>({state,revision:'fixed-v1',...Object.fromEntries(fields.map(field=>[field+'Atoms',amount]))});
const projection=(state='priced',amount='0')=>({state,revision:'fixed-v1',...Object.fromEntries(fields.map(field=>[field+'_atoms',amount]))});
const book=()=>({rates:{'synthetic-model':{input:'2',cached:'0.1',output:'10'}},aliases:[]});
const check=(args,calculation,projectionState)=>assert.deepEqual(requestPricingDiagnostic(args),{version:1,calculation,projection:projectionState});

test('详情计算状态沿用已算结果，不因没有费率书重算或改金额',()=>{
 for(const state of ['priced','manual','pending','unmetered','legacy_manual','expired']){const value={request:request(),quote:quote(state,state==='priced'?0n:null),projection:projection(state,state==='priced'?'0':null)},before=structuredClone(value);check(value,state,'consistent');assert.deepEqual(value,before);}
 const value={request:{...request(),model:'not-in-book'},quote:quote(),projection:projection(),book:{rates:{},aliases:[]}};check(value,'priced','consistent');
});

test('缺价分别识别固定版本、费率书、模型与缓存单价，保留原因不明确状态',()=>{
 const base={request:request(),quote:quote('unpriced',null),projection:projection('unpriced',null)};
 check({...base,request:{...request(),price_version:null},quote:{...base.quote,revision:null},book:null},'price_version_missing','request_version_missing');
 check({...base,book:null},'price_book_missing','book_missing');
 check({...base,book:{rates:{},aliases:[]}},'model_rate_missing','consistent');
 const noCache={rates:{'synthetic-model':{input:'2',cached:null,output:'10'}},aliases:[]};check({...base,request:{...request(),cached:1},book:noCache},'cached_rate_missing','consistent');
 check({...base,book:noCache},'unpriced_unclassified','consistent');check({...base,book:undefined},'unpriced_unclassified','consistent');
});

test('模型原因使用上游模型及原请求时间的显式别名，不猜测相近名称',()=>{
 const rates=book();rates.aliases=[{name:'synthetic-alias',target:'synthetic-model',from:50,to:150}];rates.rates['synthetic-model'].cached=null;
 const base={request:{...request(),billing_model:'synthetic-alias',cached:1},quote:quote('unpriced',null),projection:projection('unpriced',null),book:rates};check(base,'cached_rate_missing','consistent');check({...base,request:{...base.request,started:150}},'model_rate_missing','consistent');check({...base,request:{...base.request,billing_model:'synthetic-model-latest'}},'model_rate_missing','consistent');
});

test('投影缺失错版状态差异独立于详情已算金额，NULL固定版不伪称一致',()=>{
 const base={request:request(),quote:quote(),projection:projection()};check({...base,projection:null},'priced','missing');check({...base,projection:{...base.projection,revision:'different'}},'priced','revision_mismatch');check({...base,projection:projection('unpriced',null)},'priced','state_mismatch');check({...base,request:{...base.request,price_version:null}},'priced','request_version_missing');
});

test('五分项以整数精确比较，零与未知不混同，异常数值不抛出',()=>{
 const large=900719925474099312345678n,base={request:request(),quote:quote('priced',large),projection:projection('priced',large.toString())};check(base,'priced','consistent');
 for(const field of fields){check({...base,projection:{...base.projection,[field+'_atoms']:(large+1n).toString()}},'priced','amount_mismatch');for(const invalid of [undefined,null,'1.1','NaN','Infinity','1e3',Number.MAX_SAFE_INTEGER+1,{},false])check({...base,projection:{...base.projection,[field+'_atoms']:invalid}},'priced','amount_mismatch');}
 check({request:request(),quote:quote('unpriced',null),projection:projection('unpriced',null)},'unpriced_unclassified','consistent');check({request:request(),quote:quote('unpriced',null),projection:projection('unpriced','0')},'unpriced_unclassified','amount_mismatch');
 check({request:request(),quote:quote('manual',-2n),projection:projection('manual','-2')},'manual','consistent');
});

test('未知状态只输出固定枚举，不回显原模型、版本或外部文字',()=>{
 const value=requestPricingDiagnostic({request:request(),quote:quote('NEVER-OUTPUT-STATE'),projection:projection()});assert.equal(value.calculation,'unclassified');assert.equal(JSON.stringify(value).includes('NEVER-OUTPUT'),false);assert.deepEqual(requestPricingDiagnostic(),{version:1,calculation:'unclassified',projection:'request_version_missing'});
});
