import importlib.util,unittest
from pathlib import Path
from unittest.mock import Mock,patch
spec=importlib.util.spec_from_file_location('department_release',Path(__file__).parents[1]/'ci/postgres-department-release.py');release=importlib.util.module_from_spec(spec);spec.loader.exec_module(release)

class DepartmentReleaseTests(unittest.TestCase):
 def prepared(self,activate=False,installed=False):
  value=object.__new__(release.DepartmentRelease);value.activate=activate;value.plan={'binding':{'synthetic':True},'installed':installed};value.source_contracts=[release.CONTRACT]*2;value.record={'departmentIsolation':{}};value.api={'NAME':'harbor','inspect_container':lambda name:None};value.check_active=Mock();value.check_sources=Mock();value.save=Mock();value.uncertain=False;value.install_attempted=True;value.receipt=None;value.command=Mock(return_value={'contract':release.CONTRACT,'installed':installed});return value
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
  value=self.prepared(activate=True);value.command.side_effect=[{'contract':release.CONTRACT,'installed':False},{'contract':release.CONTRACT,'installed':True}];value.install();self.assertEqual([call.args[0] for call in value.command.call_args_list],['status','install']);self.assertTrue(value.receipt['installed']);self.assertFalse(value.uncertain)
  value=self.prepared(activate=True);value.api['inspect_container']=lambda name:{'id':'candidate'}
  with self.assertRaisesRegex(RuntimeError,'candidate_started_before_install'):value.install()
  value.command.assert_not_called()
 def test_unknown_commit_never_permits_blind_rollback(self):
  value=self.prepared(activate=True);value.command.side_effect=[{'contract':release.CONTRACT,'installed':False},TimeoutError(),TimeoutError()]
  with self.assertRaises(TimeoutError):value.install()
  self.assertTrue(value.uncertain)
 def test_lost_ack_recovers_installed_marker_and_preserves_failure(self):
  value=self.prepared(activate=True);value.command.side_effect=[{'contract':release.CONTRACT,'installed':False},TimeoutError(),{'contract':release.CONTRACT,'installed':True}]
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
  value=self.prepared(activate=True);value.command.return_value={'contract':release.CONTRACT,'installed':True}
  with self.assertRaisesRegex(RuntimeError,'marker_state_changed'):value.install()
  self.assertEqual([call.args[0] for call in value.command.call_args_list],['status'])

if __name__=='__main__':unittest.main()
