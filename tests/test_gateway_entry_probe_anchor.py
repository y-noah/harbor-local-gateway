"""Fixed systemd anchor identity and the real enter_probe_worker flow."""
import contextlib,copy,hashlib,importlib.util,stat,unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch
ROOT=Path(__file__).resolve().parents[1];spec=importlib.util.spec_from_file_location('probe_anchor_controller',ROOT/'ci/gateway-entry-controller.py');c=importlib.util.module_from_spec(spec);spec.loader.exec_module(c)
class ProbeAnchor(unittest.TestCase):
 @contextlib.contextmanager
 def fixture(self,change=None):
  binary=b'approved-python';digest=hashlib.sha256(binary).hexdigest();profile={'bypassProcesses':[{'pid':101,'startTicks':'old','cgroup':c.DEPLOY_CGROUP,'executableSha256':digest}]};original=copy.deepcopy(profile);unit={'Id':'harbor-entry-deploy.service','LoadState':'loaded','ActiveState':'active','SubState':'running','MainPID':'202','ControlGroup':c.DEPLOY_CGROUP,'InvocationID':'a'*32,'FragmentPath':'/etc/systemd/system/harbor-entry-deploy.service','DropInPaths':''};state={'group':'/user.slice','writes':[],'shows':0,'uid':'0\t0\t0\t0','binary':binary,'inode':73,'ticks':'new','unitUid':0,'unitMode':0o644}
  if change:change(profile,unit,state)
  def execute(args,**kwargs):
   self.assertEqual(args[:3],['/usr/bin/systemctl','show','harbor-entry-deploy.service']);state['shows']+=1;row=dict(unit)
   if state.get('changeGeneration') and state['shows']>1:row['InvocationID']='b'*32
   return '\n'.join(key+'='+value for key,value in row.items())
  def read_text(path,*args,**kwargs):
   self.assertEqual(str(path),'/proc/202/status');return 'Uid:\t'+state['uid']+'\n'
  def read_bytes(path):
   self.assertIn(str(path),['/proc/self/exe','/proc/202/exe']);return binary if str(path)=='/proc/self/exe' else state['binary']
  def info(path):
   name=str(path)
   if name=='/etc/systemd/system/harbor-entry-deploy.service':return SimpleNamespace(st_mode=stat.S_IFREG|state['unitMode'],st_uid=state['unitUid'],st_ino=99)
   return SimpleNamespace(st_mode=(stat.S_IFREG|0o644) if name.endswith('cgroup.procs') else stat.S_IFDIR|0o755,st_uid=0,st_ino=state['inode'])
  def ticks(pid):
   self.assertEqual(pid,202,'stale profile PID must never be read');return state['ticks']
  def group(pid):return state['group'] if pid==303 else state.get('anchorGroup',c.DEPLOY_CGROUP)
  def write(path,value):self.assertEqual(str(path),'/sys/fs/cgroup'+c.DEPLOY_CGROUP+'/cgroup.procs');state['writes'].append(value);state['group']=c.DEPLOY_CGROUP;return len(value)
  with contextlib.ExitStack() as stack:
   for target,name,value in [(c,'execute',execute),(c,'trusted_parents',lambda _:None),(c,'process_ticks',ticks),(c,'process_cgroup',group),(c.os,'getpid',lambda:303),(Path,'read_text',read_text),(Path,'read_bytes',read_bytes),(Path,'lstat',info),(Path,'stat',info),(Path,'write_text',write)]:stack.enter_context(patch.object(target,name,new=value))
   yield profile,unit,state,original,digest
 def test_restarted_fixed_service_is_live_anchor_without_profile_rewrite(self):
  with self.fixture() as (profile,unit,state,original,digest):
   c.EntryDeployment.enter_probe_worker(SimpleNamespace(config=profile));self.assertEqual(state['writes'],['303']);self.assertEqual(profile,original);self.assertGreaterEqual(state['shows'],2)
 def test_approved_current_scope_keeps_existing_fast_path(self):
  with self.fixture() as (profile,unit,state,*_):
   state['group']=c.DEPLOY_CGROUP;c.EntryDeployment.enter_probe_worker(SimpleNamespace(config=profile));self.assertEqual(state['writes'],[]);self.assertEqual(state['shows'],0)
 def test_unknown_or_untrusted_service_never_moves_worker(self):
  faults=[('missing',lambda p,u,s:u.update(LoadState='not-found')),('inactive',lambda p,u,s:u.update(ActiveState='inactive')),('wrong-unit',lambda p,u,s:u.update(Id='other.service')),('wrong-group',lambda p,u,s:u.update(ControlGroup='/other')),('pid-zero',lambda p,u,s:u.update(MainPID='0')),('unknown-generation',lambda p,u,s:u.update(InvocationID='')),('untrusted-unit',lambda p,u,s:s.update(unitUid=1000)),('writable-unit',lambda p,u,s:s.update(unitMode=0o666)),('nonroot-process',lambda p,u,s:s.update(uid='1000\t1000\t1000\t1000')),('executable',lambda p,u,s:s.update(binary=b'foreign')),('process-group',lambda p,u,s:s.update(anchorGroup='/other')),('service-restarted-during-check',lambda p,u,s:s.update(changeGeneration=True)),('unapproved-group',lambda p,u,s:p['bypassProcesses'][0].update(cgroup='/other')),('unapproved-executable',lambda p,u,s:p['bypassProcesses'][0].update(executableSha256='f'*64))]
  for name,change in faults:
   with self.subTest(fault=name),self.fixture(change) as (profile,unit,state,*_):
    with self.assertRaises(RuntimeError):c.EntryDeployment.enter_probe_worker(SimpleNamespace(config=profile))
    self.assertEqual(state['writes'],[])
 def test_live_pid_disappearing_is_not_replaced_with_any_matching_process(self):
  with self.fixture() as (profile,unit,state,*_),patch.object(c,'process_ticks',side_effect=FileNotFoundError):
   with self.assertRaises(FileNotFoundError):c.EntryDeployment.enter_probe_worker(SimpleNamespace(config=profile))
   self.assertEqual(state['writes'],[])
 def test_generation_change_before_move_is_rejected(self):
  with self.fixture() as (profile,unit,state,original,digest):
   witness={'pid':202,'cgroupInode':73,'startTicks':'one'}
   with patch.object(c,'live_deploy_anchor',side_effect=[witness,{**witness,'startTicks':'two'}]):
    with self.assertRaisesRegex(RuntimeError,'probe_anchor_generation_changed'):c.EntryDeployment.enter_probe_worker(SimpleNamespace(config=profile))
   self.assertEqual(state['writes'],[])
 def test_cgroup_inode_replacement_refuses_before_move(self):
  with self.fixture() as (profile,unit,state,original,digest),patch.object(c,'live_deploy_anchor',return_value={'pid':202,'cgroupInode':74,'startTicks':'new'}):
   with self.assertRaisesRegex(RuntimeError,'probe_anchor_generation_changed'):c.EntryDeployment.enter_probe_worker(SimpleNamespace(config=profile))
   self.assertEqual(state['writes'],[]);self.assertEqual(profile,original)
 def test_post_move_service_restart_is_not_reported_as_success(self):
  with self.fixture() as (profile,unit,state,original,digest):
   witness={'pid':202,'cgroupInode':73,'startTicks':'one'}
   with patch.object(c,'live_deploy_anchor',side_effect=[witness,witness,{**witness,'startTicks':'two'}]):
    with self.assertRaisesRegex(RuntimeError,'probe_scope_not_entered'):c.EntryDeployment.enter_probe_worker(SimpleNamespace(config=profile))
   self.assertEqual(state['writes'],['303']);self.assertEqual(profile,original)
if __name__=='__main__':unittest.main()
