"""Real runner lifecycle and final CI shell checks for installed releases."""
import contextlib
import importlib.util
import io
import json
import os
from pathlib import Path
import subprocess
import tempfile
import unittest
from types import SimpleNamespace
from unittest.mock import Mock,patch
ROOT=Path(__file__).parents[1]
def module(name,path):
 spec=importlib.util.spec_from_file_location(name,ROOT/path);value=importlib.util.module_from_spec(spec);spec.loader.exec_module(value);return value
u=module('progress_upgrade','ci/postgres-upgrade-deploy.py');w=module('progress_wrapper','ci/deploy-wrapper.py')
IDENTIFIER='00000000-0000-0000-0000-000000000003'
class DeploymentProgressRetryTests(unittest.TestCase):
 def entry(self):
  def save(path,value):
   temporary=path.with_suffix('.tmp');temporary.write_text(json.dumps(value));temporary.chmod(0o600);os.replace(temporary,path)
  def ticks(pid):return Path('/proc/'+str(pid)+'/stat').read_text().split(') ',1)[1].split()[19]
  return SimpleNamespace(save_json=save,private_json=lambda path:json.loads(path.read_text()),process_ticks=ticks)
 def test_failed_same_ticket_retries_archive_evidence_and_preserve_preflight_registry(self):
  with tempfile.TemporaryDirectory() as directory,patch.object(u,'RELEASES',Path(directory)):
   entry=self.entry();ticket={'deploymentId':IDENTIFIER};source={'source':'fixed'};observed=[]
   def binding(retry):observed.append(retry);return source
   def fail():
    registry=entry._harbor_preflight_directory;registry.mkdir(exist_ok=True);(registry/'owned-attempt').write_text('same-nonce')
    u.deployment_progress('preflight_execute');error=RuntimeError('synthetic private detail');error.deployment_code='preflight_execute_unknown';raise error
   for _ in range(2):
    with self.assertRaises(RuntimeError):u.supervised_upgrade(entry,ticket,fail,resume_source=binding)
   def succeed():return {'noncePreserved':(entry._harbor_preflight_directory/'owned-attempt').read_text()=='same-nonce'}
   self.assertTrue(u.supervised_upgrade(entry,ticket,succeed,resume_source=binding)['noncePreserved'])
   self.assertEqual(len(list((Path(directory)/'runners').glob(IDENTIFIER+'-failed-*'))),2);self.assertEqual(observed,[False,False,True,False,True])
 def test_retry_rejects_live_process_changed_binding_durable_or_nonfailure(self):
  for variant in ['active','source','durable','success','ticket']:
   with self.subTest(variant=variant),tempfile.TemporaryDirectory() as directory,patch.object(u,'RELEASES',Path(directory)):
    entry=self.entry();ticket={'deploymentId':IDENTIFIER};binding={'source':'same'};run=Path(directory)/'runners'/IDENTIFIER;run.mkdir(parents=True)
    entry.save_json(run/'request.json',{'version':1,'ticketDigest':u.digest(ticket),'source':binding})
    entry.save_json(run/'result.json',{'ok':variant=='success'})
    entry.save_json(run/'process.json',{'pid':os.getpid() if variant=='active' else 999999999,'startTicks':entry.process_ticks(os.getpid())})
    if variant=='durable':(Path(directory)/IDENTIFIER).mkdir()
    if variant=='ticket':ticket['extra']='changed'
    operation=Mock()
    def source(retry):return {'source':'changed'} if variant=='source' and retry else binding
    with self.assertRaises(RuntimeError):u.supervised_upgrade(entry,ticket,operation,resume_source=source)
    operation.assert_not_called();self.assertTrue(run.exists());self.assertEqual(list(run.parent.glob('*-failed-*')),[])
 def test_parent_observes_phase_and_safe_failure_without_private_message(self):
  with tempfile.TemporaryDirectory() as directory,patch.object(u,'RELEASES',Path(directory)),contextlib.redirect_stderr(io.StringIO()) as output:
   def operation():
    u.deployment_progress('preflight_create');error=RuntimeError('SECRET-MUST-STAY-PRIVATE');error.deployment_code='preflight_create_unknown';raise error
   with self.assertRaises(RuntimeError) as failure:u.supervised_upgrade(self.entry(),{'deploymentId':IDENTIFIER},operation)
   self.assertIn('preflight_create',output.getvalue());self.assertNotIn('SECRET',output.getvalue())
   self.assertEqual(failure.exception.deployment_diagnostic['code'],'preflight_create_unknown')
   self.assertNotIn('SECRET',json.dumps(w.deployment_error(failure.exception)))
 def test_failed_host_finish_reports_protected_without_replacing_original_error(self):
  for released in [True,False]:
   helper=SimpleNamespace(deployment_begin=lambda *args:'owned',deployment_finish=lambda *args:{'released':released})
   with patch.object(w,'storage_operation_helper',return_value=helper),patch.object(w,'deploy_entry_postgres',side_effect=RuntimeError('PRIVATE')),contextlib.redirect_stderr(io.StringIO()):
    with self.assertRaises(RuntimeError) as failure:w.storage_deployment({},'image',None,'deploy-entry-postgres',12)
   self.assertEqual(str(failure.exception),'PRIVATE');self.assertEqual(w.deployment_error(failure.exception)['hostOperation'],'released' if released else 'protected');self.assertNotIn('_PLANNED_OPERATION_ACTIVE',vars(w))
 def test_post_backup_phases_survive_real_runner_and_safe_wrapper_diagnostic(self):
  for phase in ['network_validation','runtime_guard','usage_plan','department_plan']:
   with self.subTest(phase=phase),tempfile.TemporaryDirectory() as directory,patch.object(u,'RELEASES',Path(directory)),contextlib.redirect_stderr(io.StringIO()):
    def operation():
     u.deployment_progress(phase);raise subprocess.TimeoutExpired('PRIVATE-COMMAND',30)
    with self.assertRaises(RuntimeError) as failure:u.supervised_upgrade(self.entry(),{'deploymentId':IDENTIFIER},operation)
    diagnostic=w.deployment_error(failure.exception)
    self.assertEqual(diagnostic['phase'],phase);self.assertEqual(diagnostic['exceptionType'],'TimeoutExpired');self.assertNotIn('PRIVATE',json.dumps(diagnostic))
 def test_progress_atomic_replace_retries_but_static_trust_error_is_not_hidden(self):
  with tempfile.TemporaryDirectory() as directory:
   path=Path(directory)/'progress.json';path.write_text('{"phase":"prepare"}');path.chmod(0o600);calls=[];original=Path.lstat
   def trusted_stat(item):
    row=original(item);return SimpleNamespace(st_dev=row.st_dev,st_ino=row.st_ino,st_mode=row.st_mode,st_uid=0,st_nlink=row.st_nlink)
   def read(item):
    calls.append(item)
    with item.open() as stream:
     if len(calls)==1:
      replacement=item.with_suffix('.next');replacement.write_text('{"phase":"finished"}');replacement.chmod(0o600);os.replace(replacement,item)
      self.assertEqual(os.fstat(stream.fileno()).st_nlink,0);raise RuntimeError('trusted reader rejected unlinked inode')
     return json.load(stream)
   with patch.object(Path,'lstat',trusted_stat):self.assertEqual(u.progress_snapshot(SimpleNamespace(private_json=read),path),{'phase':'finished'})
   self.assertEqual(len(calls),2)
   with patch.object(Path,'lstat',trusted_stat),self.assertRaisesRegex(RuntimeError,'static trust'):
    u.progress_snapshot(SimpleNamespace(private_json=Mock(side_effect=RuntimeError('static trust'))),path)
 def test_formal_supervisor_retry_freshens_failed_registered_preflight(self):
  registry=module('progress_registry_fixture','tests/test_postgres_registered_preflight.py');pg=registry.pg
  for failed_kind in ['exit','proof']:
   with self.subTest(failed_kind=failed_kind),tempfile.TemporaryDirectory() as directory,patch.object(u,'RELEASES',Path(directory)/'releases'):
    fixture=registry.RegisteredPreflightTests();api,common,rows,calls,original=fixture.fixture(directory);statefile=Path(directory)/'docker-state.json';fixed=Path(directory)/'upstream-fixed';entry=self.entry()
    def inspect(name):
     rows.clear();rows.update(json.loads(statefile.read_text()) if statefile.exists() else {});return rows.get(name)
    def docker(*args,**kwargs):
     inspect('not-used')
     try:
      result=original(*args,**kwargs)
      if not fixed.exists() and args[0]=='start' and failed_kind=='exit':next(iter(rows.values()))['State']['ExitCode']=1
      if not fixed.exists() and args[0]=='logs' and failed_kind=='proof':result.stdout='{}'
      return result
     finally:statefile.write_text(json.dumps(rows))
    api.update(docker=docker,inspect_container=inspect)
    def operation():
     api['preflight_retry_allowed']=entry._harbor_preflight_retry_allowed
     return pg.image_proof(api,registry.IMAGE,common)
    with self.assertRaises(RuntimeError):u.supervised_upgrade(entry,{'deploymentId':IDENTIFIER},operation,resume_source=lambda _: {'source':'same'})
    fixed.touch();self.assertEqual(u.supervised_upgrade(entry,{'deploymentId':IDENTIFIER},operation,resume_source=lambda _: {'source':'same'}),registry.PROOF)
    self.assertEqual(json.loads(statefile.read_text()),{});self.assertEqual(len(list((Path(directory)/'registered').glob('*-failed-*.json'))),1)

 def test_final_installed_ssh_payload_is_executed_and_exact(self):
  with tempfile.TemporaryDirectory() as directory:
   root=Path(directory);ssh=root/'ssh';ssh.write_text('#!/bin/sh\ncat > "$PAYLOAD"\n');ssh.chmod(0o700)
   image='registry.test/harbor/harbor-local-gateway:'+'a'*40
   result=subprocess.run(['bash',str(ROOT/'ci/deploy-entry-installed.sh')],env={**os.environ,'PATH':directory+':'+os.environ['PATH'],'PAYLOAD':str(root/'payload'),'HARBOR_ENTRY_INSTALLED_RELEASE':'1','DEPLOY_HOST':'gateway.test','ENTRY_INSTALLED_IMAGE':image,'HARBOR_REGISTRY_HOST':'registry.test','HARBOR_PROJECT':'harbor','CI_PROJECT_NAME':'harbor-local-gateway'},text=True,capture_output=True,timeout=5)
   self.assertEqual(result.returncode,0,result.stderr);self.assertEqual(json.loads((root/'payload').read_text()),{'action':'deploy-entry-postgres','image':image})

if __name__=='__main__':unittest.main()
