import importlib.util,unittest
from pathlib import Path
from unittest.mock import Mock
spec=importlib.util.spec_from_file_location('department_upgrade_guard',Path(__file__).parents[1]/'ci/postgres-upgrade-deploy.py');upgrade=importlib.util.module_from_spec(spec);spec.loader.exec_module(upgrade)
class DepartmentGuardTests(unittest.TestCase):
 def proof(self,installed=False):return {'department_isolation_contract':upgrade.DEPARTMENT_CONTRACT,'department_isolation_installed':installed,'quota_subject_contract':upgrade.QUOTA_SUBJECT_CONTRACT,'quota_subject_installed':installed}
 def fixture(self,proof):
  original=Mock(return_value=proof);api={'source_image_preflight':original};pg=Mock();pg.SHUTDOWN_ENV={'drainMs':'HARBOR_DRAIN'};pg.container_shutdown.return_value={'drainMs':10};return pg,api,original
 def test_malformed_contracts_and_flags_reject_before_source_checks(self):
  for value in [{'department_isolation_contract':'unknown','department_isolation_installed':False},{'department_isolation_installed':True},{'department_isolation_contract':upgrade.DEPARTMENT_CONTRACT,'department_isolation_installed':1}]:
   pg,api,original=self.fixture({})
   with self.subTest(value=value),self.assertRaises(RuntimeError):upgrade.department_upgrade_guard(pg,api,{}, {},{},[],value)
   original.assert_not_called()
  pg,api,original=self.fixture({})
  with self.assertRaisesRegex(RuntimeError,'activation_flag'):upgrade.department_upgrade_guard(pg,api,{'activateDepartmentIsolation':'true'},{},{},[],self.proof())
  original.assert_not_called()
 def test_default_bridge_accepts_old_sources_and_returns_both_real_proofs(self):
  pg,api,original=self.fixture({});values=upgrade.department_upgrade_guard(pg,api,{}, {'Id':'a'},{'Id':'b'},['--env','HARBOR_DRAIN=20'],self.proof())
  self.assertEqual(values,[{},{}]);self.assertEqual(original.call_count,2);self.assertEqual(original.call_args.args[1],['--env','HARBOR_DRAIN=10'])
 def test_first_activation_requires_compatible_source_before_stopping(self):
  pg,api,original=self.fixture({})
  with self.assertRaisesRegex(RuntimeError,'bridge_release_required'):upgrade.department_upgrade_guard(pg,api,{'activateDepartmentIsolation':True},{},{},[],self.proof())
  pg.deploy.assert_not_called()
  pg,api,original=self.fixture(self.proof());self.assertEqual(len(upgrade.department_upgrade_guard(pg,api,{'activateDepartmentIsolation':True},{},{},[],self.proof())),2)
 def test_installed_source_rejects_old_target_even_without_activation(self):
  pg,api,original=self.fixture(self.proof(True));self.assertEqual(upgrade.department_upgrade_guard(pg,api,{}, {},{},[],{}),[])
  with self.assertRaisesRegex(RuntimeError,'installed_requires_compatible_target'):api['source_image_preflight']({},[])
 def test_compatible_source_retains_original_preflight_failures(self):
  pg,api,original=self.fixture(self.proof());original.side_effect=RuntimeError('synthetic source failure')
  with self.assertRaisesRegex(RuntimeError,'synthetic source failure'):upgrade.department_upgrade_guard(pg,api,{}, {},{},[],self.proof())

 def test_subject_contract_required_before_activation_and_after_install(self):
  old={'department_isolation_contract':upgrade.DEPARTMENT_CONTRACT,'department_isolation_installed':False}
  pg,api,original=self.fixture(self.proof())
  with self.assertRaisesRegex(RuntimeError,'quota_subject_activation_target_unsupported'):upgrade.department_upgrade_guard(pg,api,{'activateDepartmentIsolation':True},{},{},[],old)
  original.assert_not_called()
  pg,api,original=self.fixture(old)
  with self.assertRaisesRegex(RuntimeError,'quota_subject_bridge_release_required'):upgrade.department_upgrade_guard(pg,api,{'activateDepartmentIsolation':True},{},{},[],self.proof())
  pg,api,original=self.fixture(old)
  with self.assertRaisesRegex(RuntimeError,'quota_subject_bridge_release_required'):upgrade.department_upgrade_guard(pg,api,{}, {},{},[],self.proof(True))
 def test_unknown_subject_marker_contract_is_not_a_legacy_bridge(self):
  for fields in [{'quota_subject_installed':True},{'quota_subject_contract':'unknown','quota_subject_installed':False},{'quota_subject_contract':upgrade.QUOTA_SUBJECT_CONTRACT,'quota_subject_installed':1}]:
   pg,api,original=self.fixture({})
   with self.assertRaises(RuntimeError):upgrade.department_upgrade_guard(pg,api,{}, {},{},[],{**{'department_isolation_contract':upgrade.DEPARTMENT_CONTRACT,'department_isolation_installed':False},**fields})
   original.assert_not_called()

if __name__=='__main__':unittest.main()
