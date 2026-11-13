"""Fixed usage-rollup repair inside an already approved same-database release."""
from pathlib import Path
import copy,ipaddress,json,os,re,selectors,shlex,stat,subprocess,uuid
from urllib.parse import urlsplit,parse_qs,unquote

CONTRACT='v1'
RUNTIME_WITNESS="""import {PostgresStore} from '/app/postgres-store.mjs';
const s=new PostgresStore({connectionString:process.env.HARBOR_DATABASE_URL,poolSize:1});let c;
try{c=await s.pool.connect();const {rows:[r]}=await c.query('SELECT (SELECT oid::text FROM pg_database WHERE datname=current_database()) database_oid,current_user::regrole::oid::text current_role_oid,session_user::regrole::oid::text session_role_oid,pg_backend_pid()::text backend_pid');console.log(JSON.stringify({databaseOid:r.database_oid,currentRoleOid:r.current_role_oid,sessionRoleOid:r.session_role_oid,backendPid:r.backend_pid}));await new Promise((resolve,reject)=>{process.stdin.resume();process.stdin.once('end',resolve);process.stdin.once('error',reject);setTimeout(()=>reject(Error('witness_timeout')),120000).unref();});}finally{c?.release();await s.close();}"""


def need(ok,code):
    if not ok:raise RuntimeError('usage_cascade_'+code)


def generation(row):
    need(isinstance(row,dict) and re.fullmatch('[a-f0-9]{64}',row.get('Id','')),'container_identity')
    return {key:copy.deepcopy(row.get(key)) for key in ['Id','Image','Config','HostConfig']}


def private_env(path,key):
    path=Path(path)
    for parent in path.parents:
        info=parent.lstat();need(stat.S_ISDIR(info.st_mode) and info.st_uid==0 and not info.st_mode&0o022,'secret_parent')
    fd=os.open(path,os.O_RDONLY|os.O_NOFOLLOW|os.O_NONBLOCK)
    with os.fdopen(fd,'r') as file:
        info=os.fstat(file.fileno());need(stat.S_ISREG(info.st_mode) and info.st_uid==0 and stat.S_IMODE(info.st_mode)==0o600 and info.st_nlink==1 and info.st_size<=65536,'secret_file')
        rows=[line.removeprefix('export ').split('=',1)[1] for line in file.read(65537).splitlines() if line.removeprefix('export ').startswith(key+'=')]
    need(len(rows)==1,'secret_key');parts=shlex.split(rows[0]);need(len(parts)==1,'secret_value');return parts[0]


def trusted_path(path,*,private=False):
    path=Path(path);need(path.is_absolute(),'trusted_path_absolute')
    for parent in [*reversed(path.parents),path]:
        info=parent.lstat();need(info.st_uid==0 and not info.st_mode&0o022 and not stat.S_ISLNK(info.st_mode) and (stat.S_ISREG(info.st_mode) and info.st_nlink==1 if parent==path else stat.S_ISDIR(info.st_mode)),'trusted_path_identity')
    if private:need(stat.S_IMODE(path.stat().st_mode)==0o600,'trusted_private_mode')
    return path


def trusted_text(path,*,private=False,limit=1048576):
    path=trusted_path(path,private=private);fd=os.open(path,os.O_RDONLY|os.O_NOFOLLOW|os.O_NONBLOCK)
    with os.fdopen(fd,'r') as file:
        info=os.fstat(file.fileno());need(info.st_size<=limit,'trusted_text_budget');value=file.read(limit+1)
    need(len(value.encode())<=limit,'trusted_text_budget');return value


def authenticated_manifest(entry,ticket):
    stack=Path(ticket['materialsDirectory']);local=entry.private_json(Path('/etc/harbor-local-assurance-release.json'))
    need(local.get('mode')=='local-assurance' and local.get('runDirectory')==str(stack) and isinstance(local.get('releaseId'),str) and re.fullmatch('[a-f0-9]{8}(?:-[a-f0-9]{4}){3}-[a-f0-9]{12}',local['releaseId']),'local_material_binding')
    state=str(trusted_path(stack/'state.enc',private=True));key=str(trusted_path('/etc/harbor-storage-backup.key',private=True));trusted_path('/opt/harbor-storage-tools/app/migration-artifacts.mjs')
    facts={'state':state,'key':key,'releaseId':local['releaseId'],'cluster':ticket['database']['cluster'],'databaseOid':ticket['database']['oid']}
    reader="import {readManifest} from '/opt/harbor-storage-tools/app/migration-artifacts.mjs';import {readFileSync} from 'node:fs';const cfg="+json.dumps(facts)+";let key=readFileSync(cfg.key);try{const state=await readManifest(cfg.state,key);if(state.phase!=='verified'||state.releaseId!==cfg.releaseId||state.result?.identityAndDataVerified!==true||state.databaseOid!==cfg.databaseOid||state.result?.database?.cluster!==cfg.cluster)throw Error('usage_manifest_binding');console.log(JSON.stringify({owner:state.owner,runtime:state.runtime,runtimeOid:state.runtimeOid,database:state.database,databaseOid:state.databaseOid,releaseId:state.releaseId}));}finally{key.fill(0);}"
    result=subprocess.run([str(trusted_path('/opt/harbor-storage-tools/node/bin/node')),'--input-type=module','--eval',reader],capture_output=True,timeout=20,env={'PATH':'/usr/bin:/bin','HOME':'/root'})
    need(result.returncode==0 and len(result.stdout)<=32768,'manifest_authentication_failed');manifest=json.loads(result.stdout)
    need(isinstance(manifest,dict) and all(isinstance(manifest.get(k),str) for k in ['owner','runtime','runtimeOid','database','databaseOid','releaseId']) and all(re.fullmatch('[a-z_][a-z0-9_]{0,62}',manifest[k]) for k in ['owner','runtime']) and re.fullmatch('[0-9]{1,20}',manifest['runtimeOid']) and manifest['database']==ticket['database']['name'] and manifest['databaseOid']==ticket['database']['oid'] and manifest['releaseId']==local['releaseId'],'manifest_identity')
    return manifest


def owner_connection_fields(template,runtime,manifest,host,ssl):
    # The fixed operator URL is a login template, not the destination. Never
    # pass any of its query parameters to pg's target/role/options parser.
    need(template.scheme in ['postgres','postgresql'] and template.username,'operator_template_invalid')
    return {'host':host,'port':runtime.port or 5432,'database':manifest['database'],'user':unquote(template.username),'password':unquote(template.password) if template.password is not None else None,'ssl':ssl,'application_name':'harbor-usage-cascade-maintenance'}


def owner_transport(entry,ticket,gateway):
    runtime=[line.split('=',1)[1] for line in gateway.get('Config',{}).get('Env',[]) if line.startswith('HARBOR_DATABASE_URL=')];need(len(runtime)==1,'runtime_configuration');r=urlsplit(runtime[0]);need(r.scheme in ['postgres','postgresql'] and r.hostname and r.username,'runtime_url')
    manifest=authenticated_manifest(entry,ticket);need(unquote(r.path.lstrip('/'))==manifest['database'] and unquote(r.username)==manifest['runtime'],'runtime_manifest_binding')
    network=ticket['network'];matches=[]
    for identifier in network['infrastructureContainerIds']:
        row=entry.inspect_container(identifier);need(row and row.get('Id')==identifier,'infrastructure_identity');net=row.get('NetworkSettings',{}).get('Networks',{}).get(network['name'],{});aliases={row.get('Name','').lstrip('/'),row.get('Config',{}).get('Hostname'),net.get('IPAddress'),*(net.get('Aliases') or []),*(net.get('DNSNames') or [])}
        if r.hostname in aliases:matches.append((row,net))
    need(len(matches)==1,'database_network_ambiguous');database,net=matches[0];state=database.get('State',{});need(state.get('Running') is True and not any(state.get(k) for k in ['Paused','Restarting','Dead','OOMKilled']) and net.get('NetworkID')==network['id'],'database_network_identity')
    host=net.get('IPAddress');need(isinstance(host,str) and isinstance(ipaddress.ip_address(host),ipaddress.IPv4Address),'database_address')
    template=urlsplit(private_env('/etc/harbor-storage-operator.env','HARBOR_STORAGE_OPERATOR_ADMIN_DATABASE_URL'));params=parse_qs(template.query,keep_blank_values=True);runtime_q=parse_qs(r.query,keep_blank_values=True)
    need(all(len(v)==1 for v in [*params.values(),*runtime_q.values()]) and params.get('sslmode')==['verify-full'] and runtime_q.get('sslmode')==['verify-full'],'owner_tls_contract')
    need(not any(k in runtime_q for k in ['host','hostaddr','port','user','password','dbname','database','service','options','connectionString']),'runtime_query_target_override')
    roots=runtime_q.get('sslrootcert',[]);need(len(roots)==1 and Path(roots[0]).is_absolute(),'runtime_ca_path');ca_path=roots[0]
    mounts=[m for m in gateway.get('Mounts',[]) if m.get('Type')=='bind' and (ca_path==m.get('Destination') or ca_path.startswith(m.get('Destination','').rstrip('/')+'/'))];need(mounts,'runtime_ca_mount');mount=max(mounts,key=lambda m:len(m['Destination']));ca=trusted_text(mount['Source']+ca_path[len(mount['Destination']):]);need('BEGIN CERTIFICATE' in ca,'owner_ca_format')
    ssl={'rejectUnauthorized':True,'servername':r.hostname,'ca':ca}
    for parameter,key in [('sslcert','cert'),('sslkey','key')]:
        if parameter in params:ssl[key]=trusted_text(params[parameter][0],private=True)
    need(bool(ssl.get('cert'))==bool(ssl.get('key')),'owner_client_tls_incomplete')
    return owner_connection_fields(template,r,manifest,host,ssl),manifest


class UsageCascadeRelease:
    def __init__(self,*,api,entry,ticket,active,gateway,worker,image,proof,record,path):
        need(proof.get('usage_rollup_cascade_contract')==CONTRACT,'candidate_contract')
        self.api=api;self.entry=entry;self.ticket=copy.deepcopy(ticket);self.active=copy.deepcopy(active);self.sources=[generation(gateway),generation(worker)];self.image=image;self.image_id=api['inspect_image'](image)['Id'];self.record=record;self.path=path;self.receipt=None;self.plan=None;self.uncertain=False
        need(self.image_id in [ticket['to']['configDigest'],ticket['to']['manifestDigest']],'candidate_image')
        self.connection,self.manifest=owner_transport(entry,ticket,gateway)
        witness=None;process=None
        try:
            self.check_active();self.check_sources(stopped=False)
            process=subprocess.Popen(['/usr/bin/docker','exec','--interactive',gateway['Id'],'node','--input-type=module','--eval',RUNTIME_WITNESS],stdin=subprocess.PIPE,stdout=subprocess.PIPE,stderr=subprocess.PIPE)
            with selectors.DefaultSelector() as selector:
                selector.register(process.stdout,selectors.EVENT_READ);need(selector.select(15),'runtime_witness_timeout')
                raw=process.stdout.readline(4097);need(len(raw)<=4096 and raw.endswith(b'\n'),'runtime_witness_budget');witness=json.loads(raw)
            need(isinstance(witness,dict) and set(witness)=={'databaseOid','currentRoleOid','sessionRoleOid','backendPid'} and witness['databaseOid']==ticket['database']['oid'] and witness['currentRoleOid']==self.manifest['runtimeOid'],'runtime_witness_database')
            self.plan=self.command('cascade-plan',{'runtimeWitness':witness})
            if self.plan.get('installed',True):need(self.plan['binding']['runtimeOid']==self.manifest['runtimeOid'] and self.plan.get('coreOwnerRole')==self.manifest['owner'],'plan_manifest_role_binding')
            self.check_sources(stopped=False);self.check_active()
        finally:
            if process is not None:
                if process.stdin:process.stdin.close()
                try:process.wait(timeout=5)
                except subprocess.TimeoutExpired:process.kill();process.wait(timeout=5)
                if process.stdout:process.stdout.close()
                if process.stderr:process.stderr.close()
        self.record['usageCascade']={'plan':self.plan,'repairId':str(uuid.uuid4()),'state':'planned'};self.save()

    def save(self):self.entry.save_json(self.path/'release.json',self.record)

    def check_active(self):
        # The caller re-reads ACTIVE after enter_probe_worker; it must remain the
        # exact approved source until the ordinary publish commits the new pair.
        need(self.entry.private_json(self.entry.ACTIVE)==self.active,'active_changed')

    def check_sources(self,*,stopped):
        for suffix,expected in zip(['','-worker'],self.sources):
            name=self.api['NAME']+suffix+('-previous' if stopped else '')
            row=self.api['inspect_container'](name);need(generation(row)==expected,'source_generation_changed')
            state=row.get('State',{});need(state.get('Running') is (not stopped) and not any(state.get(key) for key in ['Paused','Restarting','Dead','OOMKilled']),'source_stop_unconfirmed')
            if stopped:need(state.get('Status')=='exited' and state.get('Pid')==0 and bool(state.get('FinishedAt')),'source_stop_unconfirmed')

    def command(self,action,cascade):
        name='harbor-usage-cascade-'+uuid.uuid4().hex;cid=None;payload=b''
        try:
            need(self.api['inspect_image'](self.image).get('Id')==self.image_id,'candidate_image_changed')
            args=['/usr/bin/docker','create','--name',name,'--label','com.harbor.maintenance='+name,'--network','host','--read-only','--cap-drop','ALL','--security-opt','no-new-privileges','--pids-limit','64','--memory','512m','--cpus','0.5','--no-healthcheck','--tmpfs','/tmp:rw,noexec,nosuid,size=16m','--interactive','--entrypoint','node',self.image_id,'/app/postgres-usage-rollups-cli.mjs',action]
            created=subprocess.run(args,capture_output=True,timeout=30);need(created.returncode==0,'maintenance_create_failed');cid=created.stdout.decode().strip();need(re.fullmatch('[a-f0-9]{64}',cid),'maintenance_container_id')
            row=self.api['inspect_container'](cid);need(row and row['Id']==cid and row['Image']==self.image_id and row.get('Config',{}).get('Labels',{}).get('com.harbor.maintenance')==name and row['HostConfig'].get('ReadonlyRootfs') is True and row['HostConfig'].get('NetworkMode')=='host' and row.get('Config',{}).get('User')=='node' and all(m.get('Type')=='tmpfs' and m.get('Destination')=='/tmp' for m in row.get('Mounts',[])),'maintenance_container_changed')
            payload=json.dumps({'connection':self.connection,'cascade':cascade}).encode();need(len(payload)<=131072,'maintenance_input_budget')
            result=subprocess.run(['/usr/bin/docker','start','--attach','--interactive',cid],input=payload,capture_output=True,timeout=75)
            need(len(result.stdout)<=131072 and len(result.stderr)<=16384,'maintenance_output_budget')
            if result.returncode!=0:raise RuntimeError('usage_cascade_maintenance_'+action.replace('-','_')+'_failed')
            value=json.loads(result.stdout);need(isinstance(value,dict) and value.get('action')==action,'maintenance_result');return value
        finally:
            payload=b''
            row=self.api['inspect_container'](name)
            if row is not None:
                need(row['Id']==cid and row['Image']==self.image_id and row.get('Config',{}).get('Labels',{}).get('com.harbor.maintenance')==name,'maintenance_cleanup_identity')
                done=subprocess.run(['/usr/bin/docker','rm','--force',cid],capture_output=True,timeout=30);need(done.returncode==0 and self.api['inspect_container'](name) is None,'maintenance_cleanup_failed')

    def repair(self):
        self.check_active();self.check_sources(stopped=True)
        if self.plan['safe']:return
        for suffix in ['','-worker']:need(self.api['inspect_container'](self.api['NAME']+suffix) is None,'candidate_started_before_repair')
        data={'binding':self.plan['binding'],'repairId':self.record['usageCascade']['repairId']}
        self.uncertain=True
        try:result=self.command('cascade-repair',data);self.receipt=result['receipt'];self.uncertain=False
        except Exception:
            status=self.command('cascade-status',data)
            if status['safe']:self.receipt=self.command('cascade-receipt',data)['receipt']
            self.uncertain=False
            if self.receipt:self.record['usageCascade'].update(state='repaired',receipt=self.receipt);self.save()
            raise
        self.record['usageCascade'].update(state='repaired',receipt=self.receipt);self.save();self.check_sources(stopped=True);self.check_active()

    def rollback(self):
        if not self.receipt:return
        self.check_active();self.check_sources(stopped=True)
        for suffix in ['','-worker']:need(self.api['inspect_container'](self.api['NAME']+suffix) is None,'candidate_still_present')
        result=self.command('cascade-rollback',{'binding':self.plan['binding'],'receipt':self.receipt});need(result.get('rolledBack') is True,'rollback_unconfirmed')
        self.record['usageCascade']['state']='rolled-back';self.save()
