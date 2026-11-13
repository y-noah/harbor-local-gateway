import test from 'node:test';
import assert from 'node:assert/strict';
import {createHash} from 'node:crypto';
import {createDepartmentScopeIssuer,requireDepartmentScope,issueDepartmentManagementKey,departmentManagementKeyDigest,defaultDepartmentId} from '../department-access.mjs';

test('部门范围只接受服务端签发实例，缺失、克隆、JSON和自报角色均拒绝',()=>{
 const issuer=createDepartmentScopeIssuer(),scope=issuer.departmentAdmin({departmentId:'department-a',credentialId:'key-a',credentialVersion:0});
 assert.equal(requireDepartmentScope(scope),scope);assert.equal(Object.isFrozen(scope),true);
 for(const forged of [null,undefined,{}, {...scope},JSON.parse(JSON.stringify(scope)),{...scope,role:'super_admin',view:'global',department_id:null}])assert.throws(()=>requireDepartmentScope(forged),{code:'department_scope_required'});
 assert.throws(()=>{scope.department_id='department-b';},TypeError);assert.equal(scope.department_id,'department-a');
 assert.equal(defaultDepartmentId,'department-default');
});
test('超管全局及系统操作均需显式许可，员工不能冒充部门管理身份',()=>{
 const issuer=createDepartmentScopeIssuer(),admin=issuer.superAdmin({credentialId:'legacy-admin',credentialVersion:0}),system=issuer.system({purpose:'pricing'}),employee=issuer.employee({departmentId:'department-a',credentialId:'employee-key',credentialVersion:1,memberId:'member-a',memberVersion:2,departmentVersion:3});
 assert.throws(()=>requireDepartmentScope(admin),{code:'department_forbidden'});assert.equal(requireDepartmentScope(admin,{allowGlobal:true}),admin);
 assert.throws(()=>requireDepartmentScope(system,{allowGlobal:true}),{code:'department_forbidden'});
 assert.equal(requireDepartmentScope(system,{allowGlobal:true,allowSystem:true,roles:['system']}),system);
 assert.throws(()=>requireDepartmentScope(employee),{code:'department_forbidden'});assert.equal(requireDepartmentScope(employee,{roles:['employee']}),employee);
 assert.equal(employee.credential_version,1);assert.equal(employee.member_version,2);assert.equal(employee.department_version,3);
});
test('部门切换只允许本签发器内超管或同部门身份，撤销不接受旧实例',()=>{
 const issuer=createDepartmentScopeIssuer(),other=createDepartmentScopeIssuer(),global=issuer.superAdmin({credentialId:'legacy-admin',credentialVersion:0}),admin=issuer.departmentAdmin({departmentId:'department-a',credentialId:'admin-a',credentialVersion:2});
 const selected=issuer.forDepartment(global,'department-b');assert.equal(selected.role,'super_admin');assert.equal(selected.view,'department');assert.equal(requireDepartmentScope(selected).department_id,'department-b');
 assert.throws(()=>issuer.forDepartment(admin,'department-b'),{code:'department_forbidden'});assert.throws(()=>other.forDepartment(global,'department-b'),{code:'department_forbidden'});
 issuer.revoke(admin);assert.throws(()=>requireDepartmentScope(admin),{code:'department_scope_required'});assert.throws(()=>issuer.forDepartment(admin,'department-a'),{code:'department_forbidden'});
});
test('签发器拒绝缺失归属、非法版本和有访问器的配置，不兜底默认部门',()=>{
 const issuer=createDepartmentScopeIssuer(),base={departmentId:'department-a',credentialId:'key-a',credentialVersion:0};
 for(const bad of [{...base,departmentId:undefined},{...base,departmentId:'../a'},{...base,credentialVersion:-1},{...base,credentialVersion:NaN},{...base,role:'super_admin'},Object.defineProperty({},'credentialId',{get(){throw Error('must not run');},enumerable:true})])assert.throws(()=>issuer.departmentAdmin(bad),{code:'department_identity_configuration'});
 assert.throws(()=>issuer.employee({...base}),{code:'department_identity_configuration'});assert.throws(()=>issuer.system({purpose:'../worker'}),{code:'department_identity_configuration'});
 for(const missing of ['memberVersion','departmentVersion']){const employee={...base,memberId:'member-a',memberVersion:0,departmentVersion:0};delete employee[missing];assert.throws(()=>issuer.employee(employee),{code:'department_identity_configuration'});}
});
test('管理密钥与员工Key分离，只提供随机原文和不可逆验证材料',()=>{
 const first=issueDepartmentManagementKey(),second=issueDepartmentManagementKey();assert.match(first.secret,/^hdm_[A-Za-z0-9_-]{43}$/);assert.notEqual(first.secret,second.secret);assert.notEqual(first.digest,second.digest);
 assert.equal(departmentManagementKeyDigest(first.secret),first.digest);assert.notEqual(first.digest,createHash('sha256').update(first.secret).digest('hex'));assert.equal(first.prefix,first.secret.slice(0,12));
 for(const invalid of ['','hg_'+first.secret.slice(4),first.secret+'x',null,{},' '+first.secret])assert.equal(departmentManagementKeyDigest(invalid),null);
});
