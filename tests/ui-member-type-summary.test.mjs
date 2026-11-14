import test from 'node:test';
import assert from 'node:assert/strict';
import vm from 'node:vm';
import {readFileSync} from 'node:fs';
const source=readFileSync(new URL('../public/app.js',import.meta.url),'utf8'),start=source.indexOf('function typeApplicationSummary('),end=source.indexOf('function showTypeEditor(',start),context=vm.createContext({fmt:String});assert.ok(start>=0&&end>start);vm.runInContext(source.slice(start,end),context);
const render=value=>{context.value=value;return vm.runInContext('typeApplicationSummary(value)',context);};
test('类型目录展示待应用与例外人数并说明可重叠，零值有效',()=>{
 assert.equal(render({configured:true,pending_members:3,exception_members:4}),'<br>待应用 3 人 · 个人例外 4 人（可重叠）');assert.match(render({configured:true,pending_members:0,exception_members:0}),/待应用 0 人/);assert.match(source,/限额：[\s\S]*?\$\{typeApplicationSummary\(type\)\}/);
});
test('待配置类型及旧API缺失计数不伪造零待应用，不接受无效计数',()=>{
 for(const type of [{configured:false,pending_members:0,exception_members:1},{configured:true},{configured:true,pending_members:'0',exception_members:0},{configured:true,pending_members:0,exception_members:-1},{configured:true,pending_members:Infinity,exception_members:0}])assert.equal(render(type),'');
});
