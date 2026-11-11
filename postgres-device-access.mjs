import {randomBytes} from 'node:crypto';
import {AppError,id,usageWindow} from './store.mjs';
import {deviceTransportProof} from './device-transport.mjs';
import {deviceDigest,deviceEnrollmentMessage,verifyDeviceEnrollment} from './device-proof.mjs';

const fail=(status,message,code='device_invalid')=>{throw new AppError(status,message,code);};
const object=(value,keys)=>value&&typeof value==='object'&&!Array.isArray(value)&&Object.keys(value).every(key=>keys.includes(key));
const identifier=value=>typeof value==='string'&&/^[a-f0-9-]{36}$/.test(value);
const identityIdentifier=value=>typeof value==='string'&&/^[a-zA-Z0-9-]{1,80}$/.test(value);
const version=value=>Number.isSafeInteger(value)&&value>=0;
const hex=value=>typeof value==='string'&&/^[a-f0-9]{64}$/.test(value);
const secret=()=>randomBytes(32).toString('base64url');
const text=(value,max)=>{if(typeof value!=='string'||!value.trim()||value.length>max||/[\0\r\n]/.test(value))fail(400,'设备名称或系统说明无效');return value.trim();};
const dto=row=>({id:row.id,member_id:row.member_id,name:row.name,system:row.system,state:row.state,pending_renewal:row.pending_renewal,hardware_state:row.hardware_state,fingerprint_source:row.fingerprint_source,
  version:row.version,credential_generation:row.credential_generation,created:row.created,approved_at:row.approved_at,verified_request_at:row.verified_request_at,last_seen:row.last_seen,replaces_device_id:row.replaces_device_id,allowed_key_ids:row.allowed_key_ids,certificate_expires:row.certificate_expires??null,certificate_valid:row.certificate_valid??false});

export function createPostgresDeviceAccess(store,{issuer,clock=Date.now,queryQueue,readStore=store,invitationMs=3600000,maxInvitationMs=86400000,challengeMs=300000,maxChallenges=10,maxDevices=1000}={}){
  const issuerConfigured=!!issuer;if(issuer!==undefined&&(!issuer||typeof issuer.inspect!=='function'||typeof issuer.issue!=='function')||typeof clock!=='function'||![invitationMs,maxInvitationMs,challengeMs,maxChallenges,maxDevices].every(Number.isSafeInteger)||invitationMs<60000||maxInvitationMs>86400000||maxInvitationMs<invitationMs||challengeMs<10000||challengeMs>600000||maxChallenges<1||maxChallenges>100||maxDevices<1||maxDevices>10000)fail(503,'设备接入配置无效','device_configuration');
  issuer??={inspect:()=>fail(503,'设备CA尚未配置，已有强制策略仍执行','device_ca_unavailable'),issue:()=>fail(503,'设备CA尚未配置，已有强制策略仍执行','device_ca_unavailable')};
  const read=(op,{signal}={})=>queryQueue?queryQueue.run(()=>readStore.transaction(op,{level:'REPEATABLE READ',readOnly:true}),{signal}):readStore.transaction(op,{level:'REPEATABLE READ',readOnly:true});
  const audit=(action,value)=>store.run('INSERT INTO harbor.audit VALUES($1,$2,$3,$4)',id(),clock(),action,JSON.stringify(value));
  async function member(memberId,{lock=false,target=store}={}){const value=await target.get('SELECT id,enabled,device_mode,device_policy_version FROM harbor.members WHERE id=$1 AND removed_at IS NULL'+(lock?' FOR UPDATE':''),memberId);if(!value)fail(404,'员工不存在','device_member_not_found');return value;}
  async function scope(){return store.transaction(async()=>{let row=await store.get("SELECT value FROM harbor.gateway_settings WHERE name='device.instance_scope'");if(!row){await store.run("INSERT INTO harbor.gateway_settings(name,value) VALUES('device.instance_scope',$1) ON CONFLICT(name) DO NOTHING",randomBytes(32).toString('hex'));row=await store.get("SELECT value FROM harbor.gateway_settings WHERE name='device.instance_scope'");}if(!hex(row.value))fail(503,'设备实例作用域损坏','device_configuration');return row.value;});}
  async function keyIds(memberId,values){
    if(values===null)return null;if(!Array.isArray(values)||values.length>maxDevices||values.some(value=>!identityIdentifier(value))||new Set(values).size!==values.length)fail(400,'设备可用Key范围无效');
    const rows=await store.all('SELECT id FROM harbor.keys WHERE member_id=$1 AND removed_at IS NULL AND id=ANY($2::text[])',memberId,values);if(rows.length!==values.length)fail(400,'设备Key范围包含其他员工或已删除Key');return values;
  }
  async function invite(payload){
    if(!object(payload,['member_id','expires_in_ms','allowed_key_ids','replaces_device_id','renews_device_id'])||!identityIdentifier(payload.member_id)||payload.replaces_device_id!==undefined&&payload.renews_device_id!==undefined)fail(400,'设备邀请参数无效');const duration=payload.expires_in_ms??invitationMs;if(!Number.isSafeInteger(duration)||duration<60000||duration>maxInvitationMs)fail(400,'设备邀请期限无效');
    return store.transaction(async()=>{
      await member(payload.member_id,{lock:true});const allowed=await keyIds(payload.member_id,payload.allowed_key_ids??null);
      if(payload.replaces_device_id!==undefined){const old=await store.get("SELECT id FROM harbor.employee_devices WHERE id=$1 AND member_id=$2 AND state IN ('approved','disabled','needs_review') FOR UPDATE",payload.replaces_device_id,payload.member_id);if(!old)fail(400,'待替换设备不存在或归属不符');}
      if(payload.renews_device_id!==undefined){const old=await store.get("SELECT id FROM harbor.employee_devices WHERE id=$1 AND member_id=$2 AND state='approved' AND pending_renewal=FALSE FOR UPDATE",payload.renews_device_id,payload.member_id);if(!old)fail(400,'待续期设备不存在、已在续期或归属不符');}
      const active=(await store.get("SELECT COUNT(*) n FROM harbor.device_invitations WHERE member_id=$1 AND consumed IS NULL AND revoked IS NULL AND expires>$2",payload.member_id,clock())).n;if(active>=maxDevices)fail(503,'此员工未完成邀请达到上限','device_invitation_capacity');
      const at=clock(),invitation=id(),token=secret();await store.run('INSERT INTO harbor.device_invitations(id,member_id,digest,created,expires,allowed_key_ids,replaces_device_id,renews_device_id) VALUES($1,$2,$3,$4,$5,$6::jsonb,$7,$8)',invitation,payload.member_id,deviceDigest(token),at,at+duration,allowed===null?null:JSON.stringify(allowed),payload.replaces_device_id??null,payload.renews_device_id??null);
      await audit('device.invite',{invitation,member:payload.member_id,expires:at+duration});return {id:invitation,token,expires:at+duration,scope:await scope()};
    });
  }
  async function challenge(payload){
    if(!object(payload,['invitation'])||typeof payload.invitation!=='string'||payload.invitation.length>200)fail(400,'设备邀请无效');
    return store.transaction(async()=>{
      const invitation=await store.get('SELECT * FROM harbor.device_invitations WHERE digest=$1 FOR UPDATE',deviceDigest(payload.invitation));
      if(!invitation||invitation.consumed!==null||invitation.revoked!==null||invitation.expires<=clock())fail(403,'邀请不存在、已使用或已失效','device_invitation_invalid');if(invitation.challenge_count>=maxChallenges)fail(429,'邀请核验次数达到上限，请重新创建邀请','device_invitation_attempts');
      const challenge=id(),nonce=secret(),expires=Math.min(clock()+challengeMs,invitation.expires);await store.run('UPDATE harbor.device_invitations SET challenge_id=$1,challenge_digest=$2,challenge_expires=$3,challenge_count=challenge_count+1 WHERE id=$4',challenge,deviceDigest(nonce),expires,invitation.id);
      return {challenge,nonce,expires,scope:await scope()};
    });
  }
  async function register(payload){
    if(!object(payload,['invitation','challenge','nonce','scope','csr','signature','name','system','fingerprint','installation_fingerprint','fingerprint_source'])||typeof payload.invitation!=='string'||payload.invitation.length>200||!identifier(payload.challenge)||typeof payload.nonce!=='string'||payload.nonce.length>200||payload.scope!==await scope())fail(400,'设备登记参数无效');
    const name=text(payload.name,80),system=text(payload.system,160),source=payload.fingerprint_source;
    if(!['smbios_uuid','unavailable'].includes(source)||source==='smbios_uuid'&&!hex(payload.fingerprint)||source==='unavailable'&&payload.fingerprint!==null||payload.installation_fingerprint!==null&&payload.installation_fingerprint!==undefined&&!hex(payload.installation_fingerprint))fail(400,'设备指纹摘要无效');
    const initial=await store.get('SELECT member_id,consumed,revoked,expires,challenge_id,challenge_digest,challenge_expires FROM harbor.device_invitations WHERE digest=$1',deviceDigest(payload.invitation));if(!initial||initial.consumed!==null||initial.revoked!==null||initial.expires<=clock()||initial.challenge_id!==payload.challenge||initial.challenge_digest!==deviceDigest(payload.nonce)||initial.challenge_expires<=clock())fail(403,'设备邀请或挑战已失效','device_invitation_invalid');
    const inspected=await issuer.inspect(payload.csr);if(!verifyDeviceEnrollment(inspected.publicKey,deviceEnrollmentMessage({...payload,name,system}),payload.signature))fail(403,'设备私钥持有证明无效','device_possession_invalid');
    return store.transaction(async()=>{
      await member(initial.member_id,{lock:true});const invitation=await store.get('SELECT * FROM harbor.device_invitations WHERE digest=$1 FOR UPDATE',deviceDigest(payload.invitation));
      if(!invitation||invitation.member_id!==initial.member_id||invitation.consumed!==null||invitation.revoked!==null||invitation.expires<=clock()||invitation.challenge_id!==payload.challenge||invitation.challenge_digest!==deviceDigest(payload.nonce)||invitation.challenge_expires<=clock())fail(403,'设备邀请或挑战已失效','device_invitation_invalid');
      if(invitation.renews_device_id){
        const current=await store.get('SELECT * FROM harbor.employee_devices WHERE id=$1 FOR UPDATE',invitation.renews_device_id);
        if(!current||current.member_id!==invitation.member_id||current.state!=='approved'||current.pending_renewal||current.public_key_sha256!==inspected.publicKeySha256||current.hardware_state!=='credentials_only'&&(current.fingerprint!==payload.fingerprint||current.installation_fingerprint!==payload.installation_fingerprint))fail(409,'续期设备、公钥或硬件已变化，请办理重新登记','device_renewal_conflict');
        const token=secret(),at=clock();await store.run('UPDATE harbor.employee_devices SET pending_renewal=TRUE,csr=$1,enrollment_digest=$2,enrollment_expires=$3,version=version+1 WHERE id=$4',payload.csr,deviceDigest(token),at+maxInvitationMs,current.id);await store.run('UPDATE harbor.device_invitations SET consumed=$1,challenge_digest=NULL WHERE id=$2',at,invitation.id);await audit('device.renewal_request',{device:current.id,member:current.member_id,generation:current.credential_generation});return {device_id:current.id,enrollment_token:token,expires:at+maxInvitationMs,state:'renewal_pending'};
      }
      if((await store.get('SELECT COUNT(*) n FROM harbor.employee_devices WHERE member_id=$1',invitation.member_id)).n>=maxDevices)fail(503,'员工设备目录达到上限','device_capacity');
      const device=id(),token=secret(),at=clock(),expires=at+maxInvitationMs;await store.run(`INSERT INTO harbor.employee_devices(id,member_id,name,system,hardware_state,fingerprint,installation_fingerprint,fingerprint_source,allowed_key_ids,public_key_sha256,csr,created,replaces_device_id,enrollment_digest,enrollment_expires)
        VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9::jsonb,$10,$11,$12,$13,$14,$15)`,device,invitation.member_id,name,system,source==='unavailable'?'unavailable':'reported',payload.fingerprint??null,payload.installation_fingerprint??null,source,invitation.allowed_key_ids===null?null:JSON.stringify(invitation.allowed_key_ids),inspected.publicKeySha256,payload.csr,at,invitation.replaces_device_id,deviceDigest(token),expires);
      await store.run('UPDATE harbor.device_invitations SET consumed=$1,challenge_digest=NULL WHERE id=$2',at,invitation.id);await audit('device.register',{device,member:invitation.member_id,hardware:source});return {device_id:device,enrollment_token:token,expires,state:'pending'};
    });
  }
  async function enrollmentStatus(payload){
    if(!object(payload,['enrollment_token'])||typeof payload.enrollment_token!=='string'||payload.enrollment_token.length>200)fail(400,'设备登记凭据无效');
    const row=await store.get(`SELECT d.id,d.state,d.pending_renewal,d.enrollment_expires,c.certificate,c.expires FROM harbor.employee_devices d LEFT JOIN harbor.device_credentials c ON c.device_id=d.id AND c.generation=d.credential_generation AND c.revoked IS NULL WHERE d.enrollment_digest=$1`,deviceDigest(payload.enrollment_token));
    if(!row||row.enrollment_expires<=clock())fail(403,'设备登记凭据无效或已失效','device_enrollment_invalid');return {device_id:row.id,state:row.pending_renewal?'renewal_pending':row.state,...(row.state==='approved'&&!row.pending_renewal&&row.certificate?{certificate:row.certificate,certificate_expires:row.expires}:{})};
  }
  async function approve(deviceId,payload){
    if(!object(payload,['expected_version','credentials_only'])||!version(payload.expected_version)||typeof payload.credentials_only!=='boolean')fail(400,'设备批准参数无效');
    const initial=await store.get('SELECT member_id FROM harbor.employee_devices WHERE id=$1',deviceId);if(!initial)fail(404,'设备不存在');
    const target=await store.transaction(async()=>{
      await member(initial.member_id,{lock:true});const value=await store.get('SELECT * FROM harbor.employee_devices WHERE id=$1 FOR UPDATE',deviceId);
      if(value.version!==payload.expected_version||!(['pending','needs_review'].includes(value.state)||value.state==='approved'&&value.pending_renewal))fail(409,'设备状态已变化','device_conflict');const fingerprint=value.state==='needs_review'?value.review_fingerprint:value.fingerprint;if(!fingerprint&&!payload.credentials_only)fail(400,'未采集到硬件摘要，须明确批准仅设备凭据绑定','device_hardware_unverified');
      await store.run('UPDATE harbor.employee_devices SET version=version+1 WHERE id=$1',deviceId);return {...value,version:value.version+1};
    });
    if(target.state==='needs_review'&&!target.pending_renewal){return store.transaction(async()=>{
      await member(target.member_id,{lock:true});const current=await store.get('SELECT * FROM harbor.employee_devices WHERE id=$1 FOR UPDATE',deviceId);if(current.version!==target.version||current.state!=='needs_review'||current.pending_renewal)fail(409,'设备在复核期间已变化','device_conflict');
      await store.run("UPDATE harbor.employee_devices SET state='approved',hardware_state=$1,fingerprint=review_fingerprint,installation_fingerprint=review_installation_fingerprint,review_fingerprint=NULL,review_installation_fingerprint=NULL,approved_at=$2,version=version+1 WHERE id=$3",payload.credentials_only?'credentials_only':'reported',clock(),deviceId);await audit('device.hardware_approve',{device:deviceId,member:current.member_id,generation:current.credential_generation,hardware:payload.credentials_only?'credentials_only':'reported'});return dto(await store.get('SELECT * FROM harbor.employee_devices WHERE id=$1',deviceId));
    });}
    const certificate=await issuer.issue({csr:target.csr,deviceId,generation:target.credential_generation+1});if(certificate.publicKeySha256!==target.public_key_sha256||!hex(certificate.fingerprint)||!Number.isSafeInteger(certificate.expires)||certificate.expires<=clock())fail(503,'设备证书与登记公钥不一致','device_certificate_invalid');
    return store.transaction(async()=>{
      await member(target.member_id,{lock:true});const current=await store.get('SELECT * FROM harbor.employee_devices WHERE id=$1 FOR UPDATE',deviceId);if(current.version!==target.version||!(['pending','needs_review'].includes(current.state)||current.state==='approved'&&current.pending_renewal))fail(409,'设备在签发期间已变化，请重新核对','device_conflict');
      if(current.replaces_device_id&&current.credential_generation===0){const old=await store.get('SELECT * FROM harbor.employee_devices WHERE id=$1 FOR UPDATE',current.replaces_device_id);if(!old||old.member_id!==current.member_id||!['approved','disabled','needs_review'].includes(old.state))fail(409,'原设备状态已变化','device_conflict');await store.run("UPDATE harbor.employee_devices SET state='replaced',pending_renewal=FALSE,version=version+1 WHERE id=$1",old.id);await store.run('UPDATE harbor.device_credentials SET revoked=$1 WHERE device_id=$2 AND revoked IS NULL',clock(),old.id);}
      const at=clock(),generation=current.credential_generation+1;await store.run('UPDATE harbor.device_credentials SET revoked=$1 WHERE device_id=$2 AND revoked IS NULL',at,deviceId);await store.run('INSERT INTO harbor.device_credentials VALUES($1,$2,$3,$4,$5,$6,NULL)',certificate.fingerprint,deviceId,generation,certificate.certificate,at,certificate.expires);
      const nextFingerprint=current.state==='needs_review'?current.review_fingerprint:current.fingerprint,nextInstallation=current.state==='needs_review'?current.review_installation_fingerprint:current.installation_fingerprint;
      await store.run("UPDATE harbor.employee_devices SET state='approved',pending_renewal=FALSE,hardware_state=$1,credential_generation=$2,approved_at=$3,enrollment_expires=$7,version=version+1,fingerprint=$5,installation_fingerprint=$6,review_fingerprint=NULL,review_installation_fingerprint=NULL WHERE id=$4",payload.credentials_only?'credentials_only':'reported',generation,at,deviceId,nextFingerprint,nextInstallation,at+maxInvitationMs);await audit('device.approve',{device:deviceId,member:current.member_id,generation,hardware:payload.credentials_only?'credentials_only':'reported',replaces:current.replaces_device_id});return dto(await store.get('SELECT * FROM harbor.employee_devices WHERE id=$1',deviceId));
    });
  }
  async function cancelRenewal(deviceId,payload){
    if(!object(payload,['expected_version'])||!version(payload.expected_version))fail(400,'取消续期参数无效');const initial=await store.get('SELECT member_id FROM harbor.employee_devices WHERE id=$1',deviceId);if(!initial)fail(404,'设备不存在');return store.transaction(async()=>{
      await member(initial.member_id,{lock:true});const current=await store.get('SELECT * FROM harbor.employee_devices WHERE id=$1 FOR UPDATE',deviceId);if(current.version!==payload.expected_version||!current.pending_renewal)fail(409,'设备没有待续期或状态已变化','device_conflict');await store.run('UPDATE harbor.employee_devices SET pending_renewal=FALSE,enrollment_expires=$1,version=version+1 WHERE id=$2',clock(),deviceId);await audit('device.renewal_cancel',{device:deviceId,member:current.member_id,generation:current.credential_generation});return dto(await store.get('SELECT * FROM harbor.employee_devices WHERE id=$1',deviceId));
    });
  }
  async function authorize(identity,request,{phase='request'}={}){
    if(!identity||!identityIdentifier(identity.id)||!identityIdentifier(identity.key_id)||!['request','admission','dispatch'].includes(phase))fail(503,'设备鉴权调用无效','device_configuration');
    const at=clock(),owner=await store.get('SELECT m.device_mode,k.device_ids,k.enabled,m.enabled member_enabled,k.expires FROM harbor.members m JOIN harbor.keys k ON k.member_id=m.id WHERE m.id=$1 AND k.id=$2 AND m.removed_at IS NULL AND k.removed_at IS NULL',identity.id,identity.key_id);
    if(!owner||!owner.enabled||!owner.member_enabled||owner.expires!==null&&owner.expires<=at)fail(403,'员工或Key已停用或到期','key_disabled');
    const proof=deviceTransportProof(request);if(proof?.invalidCertificate)fail(403,'设备证书未通过TLS验证','device_proof_invalid');
    if(!proof?.authorized){if(owner.device_mode==='enforced'||owner.device_ids!==null)fail(403,'此员工或Key需要已批准设备证明','device_required');return Object.freeze({deviceId:null,deviceName:null,deviceSystem:null,generation:null,authState:'compatible_unverified'});}
    const value=await store.get(`SELECT d.*,c.generation,c.expires certificate_expires,c.revoked FROM harbor.device_credentials c JOIN harbor.employee_devices d ON d.id=c.device_id WHERE c.fingerprint=$1`,proof.certificateFingerprint);
    if(!value||value.member_id!==identity.id||value.revoked!==null||value.certificate_expires<=at||value.generation!==value.credential_generation||value.state!=='approved')fail(403,'设备未批准、已停用、归属不符或证书已失效','device_not_authorized');
    if(owner.device_ids!==null&&!owner.device_ids.includes(value.id)||value.allowed_key_ids!==null&&!value.allowed_key_ids.includes(identity.key_id))fail(403,'此Key不可在该设备使用','device_key_not_allowed');
    if(value.hardware_state!=='credentials_only'&&(!proof.hardwareFingerprint||proof.hardwareFingerprint!==value.fingerprint||value.installation_fingerprint&&proof.installationFingerprint!==value.installation_fingerprint)){
      if(phase==='request'&&!store.context.getStore())await markHardwareReview(value.id,value.credential_generation,{fingerprint:proof.hardwareFingerprint,installationFingerprint:proof.installationFingerprint});fail(403,'硬件摘要变化或采集失败，设备待管理员核验','device_hardware_changed');
    }
    return Object.freeze({deviceId:value.id,deviceName:value.name,deviceSystem:value.system,generation:value.generation,authState:value.hardware_state==='credentials_only'?'unverified_hardware':'authenticated'});
  }
  async function markHardwareReview(deviceId,generation,{fingerprint=null,installationFingerprint=null}={}){if(fingerprint!==null&&!hex(fingerprint)||installationFingerprint!==null&&!hex(installationFingerprint))fail(503,'设备复核摘要无效','device_configuration');return store.transaction(async()=>{const row=await store.get('SELECT member_id FROM harbor.employee_devices WHERE id=$1',deviceId);if(!row)return;await member(row.member_id,{lock:true});const changed=await store.run("UPDATE harbor.employee_devices SET state='needs_review',review_fingerprint=$3,review_installation_fingerprint=$4,version=version+1 WHERE id=$1 AND credential_generation=$2 AND state='approved'",deviceId,generation,fingerprint,installationFingerprint);if(changed.changes)await audit('device.hardware_review',{device:deviceId,member:row.member_id,generation});});}
  async function markVerified(requestId){return store.transaction(async()=>{
    const row=await store.get("SELECT device_id,device_generation,started,ended FROM harbor.requests WHERE id=$1 AND status='ok' AND device_auth_state IN ('authenticated','unverified_hardware')",requestId);if(!row?.device_id)return false;
    return (await store.run("UPDATE harbor.employee_devices SET verified_request_at=COALESCE(verified_request_at,$1),last_seen=GREATEST(COALESCE(last_seen,0),$1) WHERE id=$2 AND credential_generation=$3 AND state='approved'",row.ended??row.started,row.device_id,row.device_generation)).changes===1;
  });}
  async function policy(memberId,payload){return store.transaction(async()=>{
    const current=await member(memberId,{lock:payload!==undefined});if(payload!==undefined){if(!object(payload,['mode','expected_version'])||!['compatible','enforced'].includes(payload.mode)||!version(payload.expected_version))fail(400,'员工设备策略无效');if(current.device_policy_version!==payload.expected_version)fail(409,'员工设备策略已变化','device_policy_conflict');if(payload.mode==='enforced'&&!await store.get("SELECT id FROM harbor.employee_devices WHERE member_id=$1 AND state='approved' AND verified_request_at IS NOT NULL LIMIT 1",memberId))fail(409,'请先批准设备并完成一次真实模型调用验证','device_not_verified');await store.run('UPDATE harbor.members SET device_mode=$1,device_policy_version=device_policy_version+1 WHERE id=$2',payload.mode,memberId);await audit('device.policy',{member:memberId,mode:payload.mode,previous:current.device_mode});}
    return {member_id:memberId,mode:payload?.mode??current.device_mode,version:current.device_policy_version+(payload?1:0)};
  });}
  async function saveKeyPolicy(keyId,payload){
    if(!object(payload,['device_ids','expected_version'])||!version(payload.expected_version)||payload.device_ids!==null&&(!Array.isArray(payload.device_ids)||payload.device_ids.length>maxDevices||payload.device_ids.some(value=>!identifier(value))||new Set(payload.device_ids).size!==payload.device_ids.length))fail(400,'Key设备范围无效');
    const owner=await store.get('SELECT member_id FROM harbor.keys WHERE id=$1 AND removed_at IS NULL',keyId);if(!owner)fail(404,'Key不存在');
    return store.transaction(async()=>{await member(owner.member_id,{lock:true});const key=await store.get('SELECT device_policy_version FROM harbor.keys WHERE id=$1 AND removed_at IS NULL FOR UPDATE',keyId);if(!key)fail(404,'Key不存在');if(key.device_policy_version!==payload.expected_version)fail(409,'Key设备策略已变化','device_policy_conflict');if(payload.device_ids!==null&&(await store.get("SELECT COUNT(*) n FROM harbor.employee_devices WHERE member_id=$1 AND id=ANY($2::text[]) AND state='approved'",owner.member_id,payload.device_ids)).n!==payload.device_ids.length)fail(400,'范围包含未批准或其他员工设备');await store.run('UPDATE harbor.keys SET device_ids=$1::jsonb,device_policy_version=device_policy_version+1 WHERE id=$2',payload.device_ids===null?null:JSON.stringify(payload.device_ids),keyId);await audit('device.key_policy',{member:owner.member_id,key:keyId,devices:payload.device_ids});return {device_ids:payload.device_ids,version:key.device_policy_version+1};});
  }
  async function update(deviceId,payload){
    if(!object(payload,['name','enabled','expected_version'])||!version(payload.expected_version)||payload.name===undefined&&payload.enabled===undefined||payload.enabled!==undefined&&typeof payload.enabled!=='boolean')fail(400,'设备修改参数无效');const initial=await store.get('SELECT member_id FROM harbor.employee_devices WHERE id=$1',deviceId);if(!initial)fail(404,'设备不存在');
    return store.transaction(async()=>{await member(initial.member_id,{lock:true});const value=await store.get('SELECT * FROM harbor.employee_devices WHERE id=$1 FOR UPDATE',deviceId);if(value.version!==payload.expected_version)fail(409,'设备已变化','device_conflict');let state=value.state;if(payload.enabled!==undefined){if(!['approved','disabled'].includes(state))fail(409,'此设备不能直接恢复，需重新核验','device_state_invalid');state=payload.enabled?'approved':'disabled';}await store.run('UPDATE harbor.employee_devices SET name=$1,state=$2,version=version+1 WHERE id=$3',payload.name===undefined?value.name:text(payload.name,80),state,deviceId);await audit('device.update',{device:deviceId,member:value.member_id,state});return dto(await store.get('SELECT * FROM harbor.employee_devices WHERE id=$1',deviceId));});
  }
  async function list(params,{signal}={}){
    if([...params.keys()].some(key=>!['member_id','key_id','state','page','page_size'].includes(key)||params.getAll(key).length!==1))fail(400,'设备查询参数无效');const page=Number(params.get('page')??1),pageSize=Number(params.get('page_size')??25),memberId=params.get('member_id'),keyId=params.get('key_id'),state=params.get('state');if(!Number.isSafeInteger(page)||page<1||!Number.isSafeInteger(pageSize)||pageSize<1||pageSize>100||memberId&&!identityIdentifier(memberId)||keyId&&!identityIdentifier(keyId)||state&&!['pending','approved','disabled','needs_review','replaced'].includes(state))fail(400,'设备查询参数无效');
    return read(async()=>{
      const at=clock(),window=usageWindow(at),values=[],where=['m.removed_at IS NULL'];
      const add=(sql,value)=>{values.push(value);where.push(sql.replaceAll('?',String(values.length)));};
      const owner=memberId?await member(memberId,{target:readStore}):null;
      if(memberId)add('d.member_id=$?',memberId);
      if(keyId){const key=await readStore.get('SELECT member_id,device_ids FROM harbor.keys WHERE id=$1 AND removed_at IS NULL',keyId);if(!key)fail(404,'Key不存在');add('d.member_id=$?',key.member_id);values.push(keyId);where.push('(d.allowed_key_ids IS NULL OR d.allowed_key_ids ? $'+values.length+')');if(key.device_ids!==null)add('d.id=ANY($?::text[])',key.device_ids);}
      if(state)add('d.state=$?',state);
      const from=' FROM harbor.employee_devices d JOIN harbor.members m ON m.id=d.member_id WHERE '+where.join(' AND '),total=(await readStore.get('SELECT COUNT(*) n'+from,...values)).n,totalPages=Math.max(1,Math.ceil(total/pageSize)),current=Math.min(page,totalPages),n=values.length;
      const rows=await readStore.all(`SELECT d.*,m.name member_name,c.expires certificate_expires,(c.expires>$${n+3} AND c.revoked IS NULL AND d.state='approved') certificate_valid,
        (SELECT COUNT(*) FROM harbor.requests r WHERE r.device_id=d.id AND r.usage_at>=$${n+1} AND r.usage_at<$${n+2}${keyId?' AND r.key_id=$'+(n+4):''}) request_count
        ${from.replace(' WHERE ',' LEFT JOIN harbor.device_credentials c ON c.device_id=d.id AND c.generation=d.credential_generation WHERE ')} ORDER BY d.created DESC,d.id DESC LIMIT $${n+(keyId?5:4)} OFFSET $${n+(keyId?6:5)}`,...values,window.start,window.end,at,...(keyId?[keyId]:[]),pageSize,(current-1)*pageSize);
      const requestValues=[window.start,window.end],requestWhere=['r.usage_at>=$1','r.usage_at<$2','rm.removed_at IS NULL'];
      if(memberId){requestValues.push(memberId);requestWhere.push('r.member_id=$'+requestValues.length);}
      if(keyId){requestValues.push(keyId);requestWhere.push('r.key_id=$'+requestValues.length);}
      if(state){requestValues.push(state);requestWhere.push('EXISTS(SELECT 1 FROM harbor.employee_devices sd WHERE sd.id=r.device_id AND sd.state=$'+requestValues.length+')');}
      const summary=await readStore.get(`SELECT COUNT(DISTINCT r.device_id) used_devices,COUNT(*) FILTER(WHERE r.device_auth_state IN ('compatible_unverified','historical_unknown')) unverified_requests FROM harbor.requests r JOIN harbor.members rm ON rm.id=r.member_id WHERE ${requestWhere.join(' AND ')}`,...requestValues);
      return {rows:rows.map(row=>({...dto(row),fingerprint:hex(row.fingerprint)?row.fingerprint:null,member_name:row.member_name,request_count:row.request_count})),summary:{registered_devices:total,...summary},...(owner?{member:{id:owner.id,device_mode:owner.device_mode,device_policy_version:owner.device_policy_version}}:{}),pagination:{page:current,pageSize,total,totalPages},observedAt:at};
    },{signal});
  }
  async function handle(path,method,payload,params=new URLSearchParams(),{signal}={}){
    if(path==='/api/admin/device-invitations'&&method==='POST')return {status:201,value:await invite(payload)};
    if(path==='/api/admin/devices'&&method==='GET')return {status:200,value:await list(params,{signal})};
    const renewal=path.match(/^\/api\/admin\/devices\/([a-f0-9-]{36})\/cancel-renewal$/);if(renewal&&method==='POST')return {status:200,value:await cancelRenewal(renewal[1],payload)};
    const device=path.match(/^\/api\/admin\/devices\/([a-f0-9-]{36})(\/approve)?$/);if(device&&method===(device[2]?'POST':'PATCH'))return {status:200,value:await (device[2]?approve(device[1],payload):update(device[1],payload))};
    const employee=path.match(/^\/api\/admin\/members\/([a-zA-Z0-9-]{1,80})\/device-policy$/);if(employee&&['GET','PATCH'].includes(method))return {status:200,value:await policy(employee[1],method==='PATCH'?payload:undefined)};
    const key=path.match(/^\/api\/admin\/keys\/([a-zA-Z0-9-]{1,80})\/devices$/);if(key&&method==='PATCH')return {status:200,value:await saveKeyPolicy(key[1],payload)};
    return null;
  }
  async function publicHandle(path,method,payload,request){
    if(!path.startsWith('/api/device/enroll/')||method!=='POST')return null;if(!deviceTransportProof(request)?.encrypted)fail(403,'设备登记必须通过经验证的TLS入口','device_tls_required');
    if(path==='/api/device/enroll/challenge')return {status:200,value:await challenge(payload)};if(path==='/api/device/enroll/register')return {status:201,value:await register(payload)};if(path==='/api/device/enroll/status')return {status:200,value:await enrollmentStatus(payload)};return null;
  }
  return {handle,publicHandle,invite,challenge,register,approve,cancelRenewal,authorize,policy,saveKeyPolicy,update,list,markVerified,markHardwareReview,enrollmentStatus,scope,status:()=>({issuerConfigured,requiresTrustedTls:true})};
}
