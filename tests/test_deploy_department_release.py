import importlib.util,unittest,types
from pathlib import Path
from unittest.mock import Mock,patch
spec=importlib.util.spec_from_file_location('department_release',Path(__file__).parents[1]/'ci/postgres-department-release.py');release=importlib.util.module_from_spec(spec);spec.loader.exec_module(release)

class DepartmentReleaseTests(unittest.TestCase):
 def test_install_budget_defaults_and_explicit_boundaries(self):
  self.assertEqual(release.install_timeout_seconds({}),75)
  for value in [75,180,300]:self.assertEqual(release.install_timeout_seconds({'postgres_department_install_timeout_seconds':value}),value)
  for value in [None,False,True,74,301,75.0,'300',{},[]]:
   with self.subTest(value=value):
    with self.assertRaisesRegex(RuntimeError,'install_budget_configuration'):release.install_timeout_seconds({'postgres_department_install_timeout_seconds':value})
  with self.assertRaisesRegex(RuntimeError,'install_budget_configuration'):release.install_timeout_seconds(None)
 def test_invalid_root_budget_fails_in_constructor_before_image_or_transport(self):
  entry=types.SimpleNamespace(private_json=Mock(return_value={'postgres_department_install_timeout_seconds':'300'}));inspect=Mock()
  proof={'department_isolation_contract':release.CONTRACT,'department_isolation_installed':False,'quota_subject_contract':release.QUOTA_CONTRACT,'quota_subject_installed':False}
  with patch.object(release.transport,'owner_transport') as transport:
   with self.assertRaisesRegex(RuntimeError,'install_budget_configuration'):
    release.DepartmentRelease(api={'inspect_image':inspect},entry=entry,ticket={'activateDepartmentIsolation':True,'postgres_department_install_timeout_seconds':300},active={},gateway={},worker={},image='synthetic',proof=proof,record={},path=None,source_proofs=[proof,proof])
  entry.private_json.assert_called_once_with(Path('/etc/harbor-gateway-deploy.json'));inspect.assert_not_called();transport.assert_not_called()
 def test_only_install_uses_protected_long_budget(self):
  for action,expected in [('install',300),('status',75),('plan',75)]:
   value=object.__new__(release.DepartmentRelease);value.image='synthetic';value.image_id='sha256:'+'b'*64;value.connection={};value.record={};value.save=Mock();value.install_timeout_seconds=300;cid='a'*64;observed=[]
   row={'Id':cid,'Image':value.image_id,'Config':{'Labels':{'com.harbor.maintenance':'harbor-department-release-synthetic'},'User':'node'},'HostConfig':{'ReadonlyRootfs':True,'NetworkMode':'host'},'Mounts':[],'State':{'Running':False,'Pid':0,'Status':'exited'}}
   inspections=iter([row,row,None]);value.api={'inspect_image':lambda _: {'Id':value.image_id},'inspect_container':lambda _:next(inspections)}
   def command(args,**kwargs):
    if args[1]=='create':return types.SimpleNamespace(returncode=0,stdout=cid.encode(),stderr=b'')
    if args[1]=='start':observed.append(kwargs['timeout']);return types.SimpleNamespace(returncode=0,stdout=__import__('json').dumps({'action':action}).encode(),stderr=b'')
    return types.SimpleNamespace(returncode=0,stdout=b'',stderr=b'')
   with patch.object(release.uuid,'uuid4',return_value=types.SimpleNamespace(hex='synthetic')),patch.object(release.subprocess,'run',side_effect=command):self.assertEqual(value.command(action,{}),{'action':action})
   self.assertEqual(observed,[expected]);self.assertFalse(value.maintenance_unconfirmed)

 def prepared(self,activate=False,installed=False):
  value=object.__new__(release.DepartmentRelease);value.activate=activate;value.plan={'binding':{'synthetic':True},'installed':installed};value.source_contracts=[release.CONTRACT]*2;value.source_quota_contracts=[release.QUOTA_CONTRACT]*2;value.quota_supported=True;value.record={'departmentIsolation':{}};value.api={'NAME':'harbor','inspect_container':lambda name:None};value.check_active=Mock();value.check_sources=Mock();value.save=Mock();value.uncertain=False;value.install_attempted=True;value.receipt=None;value.command=Mock(return_value={'contract':release.CONTRACT,'installed':installed,'quota_subject_contract':release.QUOTA_CONTRACT,'quota_subject_installed':installed});return value
 def test_command_nonzero_reports_only_validated_stderr_code_and_still_cleans_container(self):
  cases=[(b'{"ok":false,"code":"42501"}','_42501'),(b'{"ok":false,"code":"department_release_runtime_not_drained"}','_department_release_runtime_not_drained'),(b'raw SQL and credentials',''),(b'{"ok":false,"code":"permission denied: SELECT secret"}',''),(b'{"ok":true,"code":"42501"}',''),(b'{"ok":false,"code":"42501","sql":"secret"}',''),(b'{"ok":false,"code":123}',''),(b'\xff','')]
  for stderr,suffix in cases:
   with self.subTest(stderr=stderr):
    value=object.__new__(release.DepartmentRelease);value.image='synthetic';value.image_id='sha256:'+'b'*64;value.connection={};value.record={};value.save=Mock();removed=[];cid='a'*64
    row={'Id':cid,'Image':value.image_id,'Config':{'Labels':{'com.harbor.maintenance':'harbor-department-release-synthetic'},'User':'node'},'HostConfig':{'ReadonlyRootfs':True,'NetworkMode':'host'},'Mounts':[],'State':{'Running':False,'Pid':0,'Status':'exited'}}
    value.api={'inspect_image':lambda image:{'Id':value.image_id},'inspect_container':lambda name:None if removed else row}
    def command(args,**kwargs):
     if args[1]=='create':return types.SimpleNamespace(returncode=0,stdout=(cid+'\n').encode())
     if args[1]=='start':return types.SimpleNamespace(returncode=1,stdout=b'',stderr=stderr)
     self.assertEqual(args[1], 'rm');self.assertNotIn('--force',args);removed.append(args[-1]);return types.SimpleNamespace(returncode=0)
    with patch.object(release.uuid,'uuid4',return_value=types.SimpleNamespace(hex='synthetic')),patch.object(release.subprocess,'run',side_effect=command):
     with self.assertRaises(RuntimeError) as failure:value.command('install',{})
    self.assertEqual(str(failure.exception),'department_release_maintenance_install_failed'+suffix);self.assertEqual(removed,[cid])
 def test_install_attach_timeout_confirms_exit_before_removing_task(self):
  value=object.__new__(release.DepartmentRelease);value.image='synthetic';value.image_id='sha256:'+'b'*64;value.connection={};value.record={};value.save=Mock();cid='a'*64;removed=[];operations=[]
  row={'Id':cid,'Image':value.image_id,'Config':{'Labels':{'com.harbor.maintenance':'harbor-department-release-synthetic'},'User':'node'},'HostConfig':{'ReadonlyRootfs':True,'NetworkMode':'host'},'Mounts':[],'State':{'Running':True,'Pid':123,'Status':'running'}}
  value.api={'inspect_image':lambda image:{'Id':value.image_id},'inspect_container':lambda name:None if removed else row}
  def command(args,**kwargs):
   operations.append(args[1])
   if args[1]=='create':return types.SimpleNamespace(returncode=0,stdout=(cid+'\n').encode())
   if args[1]=='start':raise release.subprocess.TimeoutExpired('synthetic attach',75)
   if args[1]=='kill':self.assertEqual(args[2:],["--signal=SIGTERM",cid]);return types.SimpleNamespace(returncode=0)
   if args[1]=='wait':row['State']={'Running':False,'Pid':0,'Status':'exited'};return types.SimpleNamespace(returncode=0)
   self.assertEqual(args[1:3],['rm',cid]);self.assertFalse(row['State']['Running']);removed.append(cid);return types.SimpleNamespace(returncode=0)
  with patch.object(release.uuid,'uuid4',return_value=types.SimpleNamespace(hex='synthetic')),patch.object(release.subprocess,'run',side_effect=command):
   with self.assertRaises(release.subprocess.TimeoutExpired):value.command('install',{})
  self.assertEqual(operations,['create','start','kill','wait','rm']);self.assertFalse(value.maintenance_unconfirmed);self.assertTrue(value.record['departmentIsolation']['maintenance']['processExitConfirmed'])
 def test_exit_wait_failure_preserves_task_and_blocks_new_status_process(self):
  value=object.__new__(release.DepartmentRelease);value.image='synthetic';value.image_id='sha256:'+'b'*64;value.connection={};value.record={};value.save=Mock();cid='a'*64;operations=[]
  row={'Id':cid,'Image':value.image_id,'Config':{'Labels':{'com.harbor.maintenance':'harbor-department-release-synthetic'},'User':'node'},'HostConfig':{'ReadonlyRootfs':True,'NetworkMode':'host'},'Mounts':[],'State':{'Running':True,'Pid':123,'Status':'running'}}
  value.api={'inspect_image':lambda image:{'Id':value.image_id},'inspect_container':lambda name:row}
  def command(args,**kwargs):
   operations.append(args[1])
   if args[1]=='create':return types.SimpleNamespace(returncode=0,stdout=(cid+'\n').encode())
   if args[1] in ['start','wait']:raise release.subprocess.TimeoutExpired('synthetic '+args[1],kwargs['timeout'])
   self.assertEqual(args[1],'kill');return types.SimpleNamespace(returncode=0)
  with patch.object(release.uuid,'uuid4',return_value=types.SimpleNamespace(hex='synthetic')),patch.object(release.subprocess,'run',side_effect=command):
   with self.assertRaises(release.subprocess.TimeoutExpired):value.command('install',{})
   with self.assertRaisesRegex(RuntimeError,'maintenance_process_unconfirmed'):value.confirmed_status()
  self.assertEqual(operations,['create','start','kill','wait']);self.assertTrue(value.maintenance_unconfirmed);self.assertFalse(value.record['departmentIsolation']['maintenance']['processExitConfirmed'])
 def test_unconfirmed_process_never_starts_outcome_query(self):
  value=self.prepared(activate=True);value.maintenance_unconfirmed=True
  with self.assertRaisesRegex(RuntimeError,'maintenance_process_unconfirmed'):value.confirmed_status()
  value.command.assert_not_called()
 def test_install_lost_transport_waits_only_for_known_transaction_lock_then_checks_marker(self):
  value=self.prepared(activate=True);busy=RuntimeError('department_release_maintenance_status_failed_55P03');busy.maintenance_code='55P03'
  absent={'contract':release.CONTRACT,'installed':False,'quota_subject_contract':release.QUOTA_CONTRACT,'quota_subject_installed':False}
  value.command.side_effect=[absent,TimeoutError(),busy,absent]
  with patch.object(release.time,'sleep') as sleep:
   with self.assertRaises(TimeoutError):value.install()
  self.assertEqual([c.args[0] for c in value.command.call_args_list],['status','install','status','status']);sleep.assert_called_once_with(0.5);self.assertFalse(value.uncertain);self.assertTrue(value.record['departmentIsolation']['transactionEndConfirmed']);self.assertEqual(value.record['departmentIsolation']['state'],'not-installed')
 def test_install_transaction_lock_timeout_keeps_unknown_and_blocks_source_rollback(self):
  value=self.prepared(activate=True);busy=RuntimeError('busy');busy.maintenance_code='55P03';absent={'contract':release.CONTRACT,'installed':False,'quota_subject_contract':release.QUOTA_CONTRACT,'quota_subject_installed':False}
  value.command.side_effect=[absent,TimeoutError(),busy]
  with patch.object(release.time,'monotonic',side_effect=[0,76]):
   with self.assertRaisesRegex(RuntimeError,'transaction_end_unconfirmed'):value.install()
  self.assertTrue(value.uncertain)
  with self.assertRaisesRegex(RuntimeError,'install_outcome_unknown'):value.rollback_guard()
 def test_explicit_activation_rejects_old_sources_before_owner_access(self):
  for installed,activate in [(False,True),(True,False)]:
   with patch.object(release.transport,'owner_transport') as transport:
    with self.assertRaisesRegex(RuntimeError,'bridge_release_required'):
     release.DepartmentRelease(api={},entry=None,ticket={'activateDepartmentIsolation':activate},active={},gateway={},worker={},image='synthetic',proof={'department_isolation_contract':release.CONTRACT,'department_isolation_installed':installed},record={},path=None,source_proofs=[{},{}])
    transport.assert_not_called()
 def test_nonboolean_activation_rejected(self):
  with self.assertRaisesRegex(RuntimeError,'activation_flag'):
   release.DepartmentRelease(api={},entry=None,ticket={'activateDepartmentIsolation':'true'},active={},gateway={},worker={},image='synthetic',proof={'department_isolation_contract':release.CONTRACT,'department_isolation_installed':False},record={},path=None,source_proofs=[{},{}])
 def test_bridge_default_does_not_install(self):
  value=self.prepared();value.install();self.assertEqual([call.args[0] for call in value.command.call_args_list],['status']);self.assertEqual(value.record['departmentIsolation']['state'],'bridge-only');value.check_sources.assert_called_with(stopped=True)
 def test_installed_upgrade_only_verifies(self):
  value=self.prepared(installed=True);value.install();self.assertEqual([call.args[0] for call in value.command.call_args_list],['status']);self.assertEqual(value.record['departmentIsolation']['state'],'verified')
 def test_activation_after_stops_and_before_candidate(self):
  value=self.prepared(activate=True);value.command.side_effect=[{'contract':release.CONTRACT,'installed':False,'quota_subject_contract':release.QUOTA_CONTRACT,'quota_subject_installed':False},{'contract':release.CONTRACT,'installed':True,'quota_subject_contract':release.QUOTA_CONTRACT,'quota_subject_installed':True}];value.install();self.assertEqual([call.args[0] for call in value.command.call_args_list],['status','install']);self.assertTrue(value.receipt['installed']);self.assertFalse(value.uncertain)
  value=self.prepared(activate=True);value.api['inspect_container']=lambda name:{'id':'candidate'}
  with self.assertRaisesRegex(RuntimeError,'candidate_started_before_install'):value.install()
  value.command.assert_not_called()
 def test_unknown_commit_never_permits_blind_rollback(self):
  value=self.prepared(activate=True);value.command.side_effect=[{'contract':release.CONTRACT,'installed':False,'quota_subject_contract':release.QUOTA_CONTRACT,'quota_subject_installed':False},TimeoutError(),TimeoutError()]
  with self.assertRaises(TimeoutError):value.install()
  self.assertTrue(value.uncertain)
 def test_lost_ack_recovers_installed_marker_and_preserves_failure(self):
  value=self.prepared(activate=True);value.command.side_effect=[{'contract':release.CONTRACT,'installed':False,'quota_subject_contract':release.QUOTA_CONTRACT,'quota_subject_installed':False},TimeoutError(),{'contract':release.CONTRACT,'installed':True,'quota_subject_contract':release.QUOTA_CONTRACT,'quota_subject_installed':True}]
  with self.assertRaises(TimeoutError):value.install()
  self.assertFalse(value.uncertain);self.assertTrue(value.receipt['installed'])
 def test_rollback_never_removes_marker_and_blocks_unsupported_source(self):
  value=self.prepared(installed=True);value.rollback_guard();self.assertEqual([call.args[0] for call in value.command.call_args_list],['status']);self.assertEqual(value.record['departmentIsolation']['rollback'],'compatible-source')
  value=self.prepared(installed=True);value.source_contracts=[None,release.CONTRACT]
  with self.assertRaisesRegex(RuntimeError,'rollback_source_unsupported'):value.rollback_guard()
 def test_partial_source_rename_before_install_can_restore_without_ddl(self):
  value=self.prepared();value.install_attempted=False
  row=lambda name:{'Id':name*64,'Image':'synthetic','Config':{},'HostConfig':{},'State':{'Running':False,'Status':'exited','Pid':0,'FinishedAt':'synthetic'}}
  gateway,worker=row('a'),row('b');value.sources=[release.transport.generation(gateway),release.transport.generation(worker)]
  state={'harbor-previous':gateway,'harbor-worker':worker};value.api['inspect_container']=state.get;value.rollback_guard()
  self.assertEqual([call.args[0] for call in value.command.call_args_list],['status']);value.check_sources.assert_not_called()
  state['harbor']={'Id':'c'*64,'Image':'candidate','Config':{},'HostConfig':{}}
  with self.assertRaisesRegex(RuntimeError,'source_generation_changed'):value.rollback_guard()
 def test_unknown_outcome_blocks_rollback_guard(self):
  value=self.prepared();value.uncertain=True
  with self.assertRaisesRegex(RuntimeError,'install_outcome_unknown'):value.rollback_guard()
  value.command.assert_not_called()
 def test_marker_drift_blocks_activation(self):
  value=self.prepared(activate=True);value.command.return_value={'contract':release.CONTRACT,'installed':True,'quota_subject_contract':release.QUOTA_CONTRACT,'quota_subject_installed':True}
  with self.assertRaisesRegex(RuntimeError,'marker_state_changed'):value.install()
  self.assertEqual([call.args[0] for call in value.command.call_args_list],['status'])


 def test_installed_quota_with_legacy_source_blocks_rollback_even_before_department_activation(self):
  value=self.prepared();value.source_quota_contracts=[None,release.QUOTA_CONTRACT];value.command.return_value={'contract':release.CONTRACT,'installed':False,'quota_subject_contract':release.QUOTA_CONTRACT,'quota_subject_installed':True}
  with self.assertRaisesRegex(RuntimeError,'quota_rollback_source_unsupported'):value.rollback_guard()
 def test_partial_install_cannot_claim_department_complete_without_quota(self):
  value=self.prepared(activate=True);value.command.side_effect=[{'contract':release.CONTRACT,'installed':False,'quota_subject_contract':release.QUOTA_CONTRACT,'quota_subject_installed':False},{'contract':release.CONTRACT,'installed':True,'quota_subject_contract':release.QUOTA_CONTRACT,'quota_subject_installed':False},{'contract':release.CONTRACT,'installed':True,'quota_subject_contract':release.QUOTA_CONTRACT,'quota_subject_installed':False}]
  with self.assertRaisesRegex(RuntimeError,'quota_required_for_department'):value.install()
  self.assertTrue(value.uncertain)

if __name__=='__main__':unittest.main()
