"""Two-stage department activation in the authenticated same-database publisher.

The bridge release leaves the marker absent. A later explicit activation needs
both stopped source images and the candidate to support the same contract.
No rollback ever removes department data or restores an unsupported source.
"""
from pathlib import Path
import copy,importlib.util,json,re,selectors,subprocess,time,uuid

_spec=importlib.util.spec_from_file_location('department_release_cascade_transport',Path(__file__).with_name('postgres-usage-cascade.py'))
transport=importlib.util.module_from_spec(_spec);_spec.loader.exec_module(transport)
CONTRACT='harbor-department-isolation-v1'
QUOTA_CONTRACT='quota-subject-coordination-v1'

def need(ok,code):
    if not ok:raise RuntimeError('department_release_'+code)

def contract(proof):
    need(isinstance(proof,dict),'proof_required')
    value=proof.get('department_isolation_contract')
    need(value in [None,CONTRACT],'contract_unknown')
    return value

def install_timeout_seconds(config):
    need(isinstance(config,dict),'install_budget_configuration')
    value=config.get('postgres_department_install_timeout_seconds',75)
    need(type(value) is int and 75<=value<=300,'install_budget_configuration')
    return value

def unstarted_task(row,name,image_id,action):
    return transport.unstarted_maintenance_task(row,name,image_id,'/app/postgres-department-release.mjs',action,need)

class DepartmentRelease(transport.UsageCascadeRelease):
    def __init__(self,*,api,entry,ticket,active,gateway,worker,image,proof,record,path,source_proofs):
        need(contract(proof)==CONTRACT,'candidate_contract')
        activate=ticket.get('activateDepartmentIsolation',False);need(type(activate) is bool,'activation_flag')
        need(type(proof.get('department_isolation_installed')) is bool,'candidate_marker_state')
        need(isinstance(source_proofs,(list,tuple)) and len(source_proofs)==2,'source_proofs')
        self.source_contracts=[contract(p) for p in source_proofs]
        quota=proof.get('quota_subject_contract');need(quota in [None,QUOTA_CONTRACT],'quota_contract_unknown')
        self.source_quota_contracts=[p.get('quota_subject_contract') for p in source_proofs]
        need(all(p in [None,QUOTA_CONTRACT] for p in self.source_quota_contracts),'source_quota_contract_unknown')
        self.quota_supported=quota==QUOTA_CONTRACT
        if self.quota_supported:need(type(proof.get('quota_subject_installed')) is bool,'quota_marker_state')
        if activate or proof.get('quota_subject_installed',False):need(self.quota_supported and all(p==QUOTA_CONTRACT for p in self.source_quota_contracts),'quota_bridge_release_required')
        if activate or proof['department_isolation_installed']:need(all(p==CONTRACT for p in self.source_contracts),'bridge_release_required')
        self.install_timeout_seconds=install_timeout_seconds(entry.private_json(Path('/etc/harbor-gateway-deploy.json')))
        self.activate=activate;self.api=api;self.entry=entry;self.ticket=copy.deepcopy(ticket);self.active=copy.deepcopy(active);self.sources=[transport.generation(gateway),transport.generation(worker)];self.image=image;self.image_id=api['inspect_image'](image)['Id'];self.record=record;self.path=path;self.receipt=None;self.plan=None;self.uncertain=False;self.install_attempted=False
        need(self.image_id in [ticket['to']['configDigest'],ticket['to']['manifestDigest']],'candidate_image')
        self.connection,self.manifest=transport.owner_transport(entry,ticket,gateway)
        process=None
        def plan_input():
            nonlocal process
            self.check_active();self.check_sources(stopped=False)
            process=subprocess.Popen(['/usr/bin/docker','exec','--interactive',gateway['Id'],'node','--input-type=module','--eval',transport.RUNTIME_WITNESS],stdin=subprocess.PIPE,stdout=subprocess.PIPE,stderr=subprocess.PIPE)
            with selectors.DefaultSelector() as selector:
                selector.register(process.stdout,selectors.EVENT_READ);need(selector.select(15),'runtime_witness_timeout');raw=process.stdout.readline(4097);need(len(raw)<=4096 and raw.endswith(b'\n'),'runtime_witness_budget');witness=json.loads(raw)
            expected={key:self.manifest[key] for key in ['owner','runtime','runtimeOid','databaseOid']}
            return {'expected':expected,'runtimeWitness':witness}
        try:
            self.check_active();self.check_sources(stopped=False)
            self.plan=self.command('plan',plan_input)
            need(self.plan.get('contract')==CONTRACT and self.plan.get('installed') is proof['department_isolation_installed'],'marker_state_changed')
            if self.quota_supported:need(self.plan.get('quota_subject_contract')==QUOTA_CONTRACT and self.plan.get('quota_subject_installed') is proof['quota_subject_installed'],'quota_marker_state_changed')
            self.check_sources(stopped=False);self.check_active()
        finally:
            if process is not None:
                if process.stdin:process.stdin.close()
                try:process.wait(timeout=5)
                except subprocess.TimeoutExpired:process.kill();process.wait(timeout=5)
                if process.stdout:process.stdout.close()
                if process.stderr:process.stderr.close()
        self.record['departmentIsolation']={'plan':self.plan,'activate':activate,'state':'planned'};self.save()

    def command(self,action,cascade):
        need(not getattr(self,'maintenance_unconfirmed',False),'maintenance_process_unconfirmed')
        name='harbor-department-release-'+uuid.uuid4().hex;cid=None;payload=b'';start_attempted=False
        try:
            need(self.api['inspect_image'](self.image).get('Id')==self.image_id,'candidate_image_changed')
            args=['/usr/bin/docker','create','--name',name,'--label','com.harbor.maintenance='+name,'--network','host','--read-only','--cap-drop','ALL','--security-opt','no-new-privileges','--pids-limit','64','--memory','512m','--cpus','0.5','--no-healthcheck','--tmpfs','/tmp:rw,noexec,nosuid,size=16m','--interactive','--entrypoint','node',self.image_id,'/app/postgres-department-release.mjs',action]
            self.maintenance_unconfirmed=True
            cid,reconciled=transport.create_maintenance_task(self.api,args,name,self.image_id,'/app/postgres-department-release.mjs',action,need)
            row=self.api['inspect_container'](cid);need(row and row['Id']==cid and row['Image']==self.image_id and row.get('Config',{}).get('Labels',{}).get('com.harbor.maintenance')==name and row['HostConfig'].get('ReadonlyRootfs') is True and row['HostConfig'].get('NetworkMode')=='host' and row.get('Config',{}).get('User')=='node' and all(m.get('Type')=='tmpfs' and m.get('Destination')=='/tmp' for m in row.get('Mounts',[])),'maintenance_container_changed')
            if reconciled:need(unstarted_task(row,name,self.image_id,action)==cid,'maintenance_container_changed')
            self.maintenance_unconfirmed=True
            if callable(cascade):cascade=cascade()
            payload=json.dumps({'connection':self.connection,'cascade':cascade}).encode();need(len(payload)<=131072,'maintenance_input_budget')
            budget=getattr(self,'install_timeout_seconds',75) if action=='install' else 75
            start_attempted=True
            result=subprocess.run(['/usr/bin/docker','start','--attach','--interactive',cid],input=payload,capture_output=True,timeout=budget)
            need(len(result.stdout)<=131072 and len(result.stderr)<=16384,'maintenance_output_budget')
            if result.returncode!=0:
                code=None
                try:
                    error=json.loads(result.stderr)
                    if isinstance(error,dict) and set(error)=={'ok','code'} and error['ok'] is False and isinstance(error['code'],str) and re.fullmatch('[A-Za-z0-9_]{1,80}',error['code']):code=error['code']
                except (ValueError,UnicodeError):pass
                error=RuntimeError('department_release_maintenance_'+action.replace('-','_')+'_failed'+('_'+code if code else ''));error.maintenance_code=code;raise error
            value=json.loads(result.stdout);need(isinstance(value,dict) and value.get('action')==action,'maintenance_result');return value
        finally:
            payload=b''
            row=self.api['inspect_container'](name)
            if row is not None or cid is not None:
                self.maintenance_unconfirmed=True
                if cid is None:
                    # A create timeout loses the acknowledgement, not necessarily
                    # the container. Discover only this exact unstarted task;
                    # never start it or infer that the maintenance plan ran.
                    cid=unstarted_task(row,name,self.image_id,action)
                diagnostic={'action':action,'containerId':cid,'processExitConfirmed':False}
                self.record.setdefault('departmentIsolation',{})['maintenance']=diagnostic;self.save()
                def owned(value):
                    need(value is not None and value['Id']==cid and value['Image']==self.image_id and value.get('Config',{}).get('Labels',{}).get('com.harbor.maintenance')==name,'maintenance_cleanup_identity')
                    return value['State']
                state=owned(row)
                if not start_attempted:
                    need(state.get('Running') is False and state.get('Pid')==0 and state.get('Status')=='created' and not any(state.get(k) for k in ['Paused','Restarting','Dead']),'maintenance_creation_unconfirmed')
                if state.get('Running') is True:
                    # Cancel only this authenticated task. Docker CLI loss does
                    # not prove that the container or its PG transaction exited.
                    subprocess.run(['/usr/bin/docker','kill','--signal=SIGTERM',cid],capture_output=True,timeout=30)
                    waited=subprocess.run(['/usr/bin/docker','wait',cid],capture_output=True,timeout=30)
                    need(waited.returncode==0,'maintenance_exit_unconfirmed');state=owned(self.api['inspect_container'](cid))
                need(state.get('Running') is False and state.get('Pid')==0 and state.get('Status')==('exited' if start_attempted else 'created') and not any(state.get(k) for k in ['Paused','Restarting','Dead']),'maintenance_exit_unconfirmed')
                diagnostic['processExitConfirmed']=True;self.save()
                done=subprocess.run(['/usr/bin/docker','rm',cid],capture_output=True,timeout=30);need(done.returncode==0 and self.api['inspect_container'](name) is None,'maintenance_cleanup_failed')
                self.maintenance_unconfirmed=False

    def confirmed_status(self):
        # status holds the same transaction advisory lock as install. A success
        # proves the previous install transaction released its lock and reports
        # its actual committed marker; container exit alone cannot prove this.
        need(not getattr(self,'maintenance_unconfirmed',False),'maintenance_process_unconfirmed')
        deadline=time.monotonic()+75
        while True:
            try:
                result=self.status()
                self.record.setdefault('departmentIsolation',{})['transactionEndConfirmed']=True;self.save();return result
            except RuntimeError as error:
                if getattr(error,'maintenance_code',None)!='55P03':raise
                if time.monotonic()>=deadline:raise RuntimeError('department_release_transaction_end_unconfirmed') from None
                time.sleep(0.5)

    def status(self):
        result=self.command('status',{'binding':self.plan['binding']})
        need(result.get('contract')==CONTRACT and type(result.get('installed')) is bool,'status_unknown')
        if self.quota_supported:need(result.get('quota_subject_contract')==QUOTA_CONTRACT and type(result.get('quota_subject_installed')) is bool,'quota_status_unknown')
        if result.get('installed'):need(result.get('quota_subject_installed') is True,'quota_required_for_department')
        return result

    def install(self):
        self.check_active();self.check_sources(stopped=True)
        for suffix in ['','-worker']:need(self.api['inspect_container'](self.api['NAME']+suffix) is None,'candidate_started_before_install')
        self.install_attempted=True
        state=self.status();need(state['installed'] is self.plan['installed'],'marker_state_changed')
        if state['installed'] or not self.activate:
            self.record['departmentIsolation']['state']='verified' if state['installed'] else 'bridge-only';self.save();return
        need(all(value==CONTRACT for value in self.source_contracts),'bridge_release_required')
        self.uncertain=True
        try:
            result=self.command('install',{'binding':self.plan['binding'],'activate':True})
            need(result.get('contract')==CONTRACT and result.get('installed') is True,'install_unconfirmed')
            need(result.get('quota_subject_contract')==QUOTA_CONTRACT and result.get('quota_subject_installed') is True,'quota_install_unconfirmed')
            self.receipt=result;self.uncertain=False
        except Exception:
            status=self.confirmed_status();self.uncertain=False
            if status['installed']:self.receipt=status
            self.record['departmentIsolation'].update(state='installed' if self.receipt else 'not-installed',receipt=self.receipt);self.save();raise
        self.record['departmentIsolation'].update(state='installed',receipt=self.receipt);self.save();self.check_sources(stopped=True);self.check_active()

    def rollback_guard(self):
        self.check_active();need(not self.uncertain,'install_outcome_unknown')
        if self.install_attempted:
            self.check_sources(stopped=True)
            for suffix in ['','-worker']:need(self.api['inspect_container'](self.api['NAME']+suffix) is None,'candidate_still_present')
        else:
            # stop/rename may fail partway, before this extension is entered.
            # Accept only the saved source generations, wherever rename left them.
            for suffix,expected in zip(['','-worker'],self.sources):
                name=self.api['NAME']+suffix;rows=[row for row in [self.api['inspect_container'](name),self.api['inspect_container'](name+'-previous')] if row is not None]
                need(len(rows)==1 and transport.generation(rows[0])==expected,'source_generation_changed')
                state=rows[0].get('State',{});need(type(state.get('Running')) is bool and not any(state.get(key) for key in ['Paused','Restarting','Dead','OOMKilled']),'source_state_unknown')
                if not state['Running']:need(state.get('Status')=='exited' and state.get('Pid')==0 and bool(state.get('FinishedAt')),'source_stop_unconfirmed')
        status=self.status()
        if status['installed']:need(all(value==CONTRACT for value in self.source_contracts),'rollback_source_unsupported')
        if status.get('quota_subject_installed'):need(all(value==QUOTA_CONTRACT for value in self.source_quota_contracts),'quota_rollback_source_unsupported')
        self.record['departmentIsolation']['rollback']='compatible-source' if status['installed'] else 'legacy-source';self.save()
