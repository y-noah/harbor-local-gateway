"""Two-stage department activation in the authenticated same-database publisher.

The bridge release leaves the marker absent. A later explicit activation needs
both stopped source images and the candidate to support the same contract.
No rollback ever removes department data or restores an unsupported source.
"""
from pathlib import Path
import copy,importlib.util,json,re,selectors,subprocess,uuid

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
        self.activate=activate;self.api=api;self.entry=entry;self.ticket=copy.deepcopy(ticket);self.active=copy.deepcopy(active);self.sources=[transport.generation(gateway),transport.generation(worker)];self.image=image;self.image_id=api['inspect_image'](image)['Id'];self.record=record;self.path=path;self.receipt=None;self.plan=None;self.uncertain=False;self.install_attempted=False
        need(self.image_id in [ticket['to']['configDigest'],ticket['to']['manifestDigest']],'candidate_image')
        self.connection,self.manifest=transport.owner_transport(entry,ticket,gateway)
        process=None
        try:
            self.check_active();self.check_sources(stopped=False)
            process=subprocess.Popen(['/usr/bin/docker','exec','--interactive',gateway['Id'],'node','--input-type=module','--eval',transport.RUNTIME_WITNESS],stdin=subprocess.PIPE,stdout=subprocess.PIPE,stderr=subprocess.PIPE)
            with selectors.DefaultSelector() as selector:
                selector.register(process.stdout,selectors.EVENT_READ);need(selector.select(15),'runtime_witness_timeout');raw=process.stdout.readline(4097);need(len(raw)<=4096 and raw.endswith(b'\n'),'runtime_witness_budget');witness=json.loads(raw)
            expected={key:self.manifest[key] for key in ['owner','runtime','runtimeOid','databaseOid']}
            self.plan=self.command('plan',{'expected':expected,'runtimeWitness':witness})
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
        name='harbor-department-release-'+uuid.uuid4().hex;cid=None;payload=b''
        try:
            need(self.api['inspect_image'](self.image).get('Id')==self.image_id,'candidate_image_changed')
            args=['/usr/bin/docker','create','--name',name,'--label','com.harbor.maintenance='+name,'--network','host','--read-only','--cap-drop','ALL','--security-opt','no-new-privileges','--pids-limit','64','--memory','512m','--cpus','0.5','--no-healthcheck','--tmpfs','/tmp:rw,noexec,nosuid,size=16m','--interactive','--entrypoint','node',self.image_id,'/app/postgres-department-release.mjs',action]
            created=subprocess.run(args,capture_output=True,timeout=30);need(created.returncode==0,'maintenance_create_failed');cid=created.stdout.decode().strip();need(re.fullmatch('[a-f0-9]{64}',cid),'maintenance_container_id')
            row=self.api['inspect_container'](cid);need(row and row['Id']==cid and row['Image']==self.image_id and row.get('Config',{}).get('Labels',{}).get('com.harbor.maintenance')==name and row['HostConfig'].get('ReadonlyRootfs') is True and row['HostConfig'].get('NetworkMode')=='host' and row.get('Config',{}).get('User')=='node' and all(m.get('Type')=='tmpfs' and m.get('Destination')=='/tmp' for m in row.get('Mounts',[])),'maintenance_container_changed')
            payload=json.dumps({'connection':self.connection,'cascade':cascade}).encode();need(len(payload)<=131072,'maintenance_input_budget')
            result=subprocess.run(['/usr/bin/docker','start','--attach','--interactive',cid],input=payload,capture_output=True,timeout=75)
            need(len(result.stdout)<=131072 and len(result.stderr)<=16384,'maintenance_output_budget')
            if result.returncode!=0:raise RuntimeError('department_release_maintenance_'+action.replace('-','_')+'_failed')
            value=json.loads(result.stdout);need(isinstance(value,dict) and value.get('action')==action,'maintenance_result');return value
        finally:
            payload=b''
            row=self.api['inspect_container'](name)
            if row is not None:
                need(row['Id']==cid and row['Image']==self.image_id and row.get('Config',{}).get('Labels',{}).get('com.harbor.maintenance')==name,'maintenance_cleanup_identity')
                done=subprocess.run(['/usr/bin/docker','rm','--force',cid],capture_output=True,timeout=30);need(done.returncode==0 and self.api['inspect_container'](name) is None,'maintenance_cleanup_failed')

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
            status=self.status();self.uncertain=False
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
