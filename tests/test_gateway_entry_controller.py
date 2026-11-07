import importlib.util
import copy
import unittest
import stat
from types import SimpleNamespace
from pathlib import Path
from unittest.mock import patch

ROOT=Path(__file__).resolve().parents[1]
def load(name,path):
 spec=importlib.util.spec_from_file_location(name,path);module=importlib.util.module_from_spec(spec);spec.loader.exec_module(module);return module
controller=load('gateway_entry_controller',ROOT/'ci/gateway-entry-controller.py')
bridge=load('gateway_legacy_transparent',ROOT/'ci/gateway-legacy-transparent.py')

class EntryBoundaryTests(unittest.TestCase):
 def test_withdraw_retries_after_ports_flushed_and_control_reply_failed(self):
  config={'source':{'port':8092}};state={'configDigest':controller.fingerprint(config),'phase':'holding'};enabled=[8092];first=True
  def resources(current,policy,**kwargs):
   self.assertTrue(enabled==[8092] or current['phase']=='withdrawing' and enabled==[])
  def control(command=None,epoch=None,slot=None):
   nonlocal first
   if first:first=False;raise ConnectionResetError('controlled failure after flush')
   return {'phase':'active' if command=='resume' else 'holding','slot':'legacy','epoch':5 if command=='resume' else 4}
  def persist(value):state.clear();state.update(copy.deepcopy(value))
  def load(path):return copy.deepcopy(state)
  def flush(body):enabled.clear()
  with patch.object(controller,'private_json',side_effect=load),patch.object(controller,'verify_resources',side_effect=resources),patch.object(controller,'verify_environment'),patch.object(controller,'verify_source'),patch.object(controller,'save_state',side_effect=persist),patch.object(controller,'nft',side_effect=flush),patch.object(controller,'entry_state',side_effect=control):
   with self.assertRaises(ConnectionResetError):controller.run_action(config,'withdraw')
   self.assertEqual(state['phase'],'withdrawing');self.assertEqual(enabled,[])
   result=controller.run_action(config,'withdraw')
   self.assertEqual(result['phase'],'withdrawn');self.assertTrue(result['existingNatAndTcRetained']);self.assertEqual(state['phase'],'withdrawn')

 def test_status_only_waits_for_fixed_typed_unavailable_reply(self):
  image='registry.example/harbor:'+'a'*40
  results=[(75,'{"code":"entry_status_unavailable"}','active_database_not_ready'),(75,'{"code":"entry_status_denied"}','active_database_status_denied'),(1,'{"code":"entry_status_denied"}','active_database_status_denied')]
  for status,diagnostic,expected in results:
   result=SimpleNamespace(returncode=status,stdout='',stderr=diagnostic)
   with self.subTest(status=status,diagnostic=diagnostic),patch.object(controller,'trusted_parents'),patch.object(Path,'lstat',return_value=SimpleNamespace(st_mode=stat.S_IFREG|0o644,st_uid=0)),patch.object(controller.subprocess,'run',return_value=result) as invoked:
    with self.assertRaisesRegex(RuntimeError,expected):controller.read_active_authorization(image)
    args,kwargs=invoked.call_args;self.assertEqual(args[0],['/opt/harbor-storage-tools/node/bin/node','/usr/local/lib/harbor/gateway-entry-activation-status.mjs']);self.assertEqual(set(kwargs['env']),{'PATH','LANG'});self.assertEqual(__import__('json').loads(kwargs['input']),{'image':image})

 def test_public_ipv4_can_never_be_remapped_by_legacy_proxy_policy(self):
  config={'version':1,'socketPath':'/run/harbor-entry/legacy.sock','sourceAddress':'10.62.0.2','sourcePort':8092,'maxConnections':1,'maxHeaderBytes':4096,'headerTimeoutMs':1000,'connectTimeoutMs':1000,'legacyEffectivePeers':[{'clientNetwork':'203.0.113.0/24','peerAddress':'10.62.0.1','witnessSha256':'a'*64}]}
  with self.assertRaisesRegex(ValueError,'peer_mapping_public_ipv4'):bridge.configuration(config)

 def test_ipv6_or_loopback_without_root_witness_fails_closed(self):
  for value in ['::1','127.0.0.1','2001:db8::1']:
   with self.subTest(value=value),self.assertRaisesRegex(ValueError,'identity_unwitnessed_proxy'):
    bridge.client_address(('GET / HTTP/1.1\r\nx-harbor-entry-client-address: '+value+'\r\n\r\n').encode())
  self.assertEqual(bridge.client_address(b'GET / HTTP/1.1\r\nx-harbor-entry-client-address: ::ffff:203.0.113.9\r\n\r\n'),'203.0.113.9')

 def test_candidate_rejects_foreign_generation_even_with_healthy_state(self):
  deployment=object.__new__(controller.EntryDeployment);deployment.image='registry.example/harbor:'+'a'*40
  deployment.approval={'configDigest':'sha256:'+'b'*64,'manifestDigest':'sha256:'+'c'*64,'deploymentId':'synthetic','receiptDigest':'d'*64}
  row={'Image':'sha256:'+'b'*64,'Config':{'Image':deployment.image,'Labels':{'com.aistrong.project':'harbor-local-gateway','com.harbor.activation-deployment-id':'another-generation','com.harbor.activation-receipt':'d'*64}},'State':{'Running':True,'Health':{'Status':'healthy'}}}
  with self.assertRaisesRegex(RuntimeError,'candidate_activation_changed'):deployment.verify_candidate_row(row)
  row['Config']['Labels']['com.harbor.activation-deployment-id']='synthetic';deployment.verify_candidate_row(row)
  row['Image']='sha256:'+'e'*64
  with self.assertRaisesRegex(RuntimeError,'candidate_image_changed'):deployment.verify_candidate_row(row)

 def test_candidate_address_must_be_unique_and_comes_from_inspection(self):
  deployment=object.__new__(controller.EntryDeployment)
  self.assertEqual(deployment.endpoint({'NetworkSettings':{'Networks':{'owned':{'IPAddress':'10.62.0.3'}}}}),'10.62.0.3')
  with self.assertRaisesRegex(RuntimeError,'candidate_address_ambiguous'):deployment.endpoint({'NetworkSettings':{'Networks':{'one':{'IPAddress':'10.62.0.3'},'two':{'IPAddress':'10.63.0.3'}}}})

 def test_old_socket_drain_counts_ipv4_and_ipv6_and_never_prints_tuple(self):
  data='header\n 0: 02003E0A:1F9C 03633E0A:D431 01 other\n 1: 02003E0A:1F9C 03633E0A:D432 06 other\n'
  with patch.object(Path,'exists',return_value=True),patch.object(Path,'read_text',return_value=data):
   active,counts=controller.old_socket_counts(123,8092)
  self.assertEqual(active,2);self.assertEqual(counts,{'tcp:01':1,'tcp:06':1,'tcp6:01':1,'tcp6:06':1})
  self.assertNotIn('03633E0A',str(counts))

 def test_probe_before_binding_is_rejected(self):
  deployment=object.__new__(controller.EntryDeployment);deployment.config={'source':{'port':8092}};deployment.candidate=None
  deployment.check=lambda approval=None:None
  with self.assertRaisesRegex(RuntimeError,'probe_target'):deployment.probe(8092,'/ready')

 def test_authorized_deployment_still_rejects_changed_fence_or_entry_epoch(self):
  deployment=object.__new__(controller.EntryDeployment);source={'containerId':'a'*64,'imageId':'sha256:'+'b'*64,'startedAt':'started'}
  config={'source':source,'bridge':{'name':'owned','ports':['old']}}
  state={'configDigest':controller.fingerprint(config),'phase':'holding','sourceBridgePort':'old','drainReceipt':{'entryEpoch':7}}
  fence={'scope':'docker-systemd','sourceContainerId':source['containerId'],'sourceImageId':source['imageId'],'sourceStartedAt':'started','sourceFinishedAt':'finished','sourceStopped':True,'supervisorFenced':True,'productionVerified':True}
  deployment.config=config;deployment.state=state;deployment.approval={'sourceFence':fence};deployment.activated=False;deployment.candidate=None
  current={'slot':'legacy','phase':'drained','inflight':0,'epoch':7}
  def private(path):return config if path==controller.PROFILE else state
  with patch.object(controller.os,'geteuid',return_value=0),patch.object(controller,'private_json',side_effect=private),patch.object(controller,'verify_resources'),patch.object(controller,'verify_source',return_value={'State':{'FinishedAt':'finished'}}),patch.object(controller,'execute',return_value='[]'),patch.object(controller,'verify_environment'),patch.object(controller,'entry_state',side_effect=lambda:current):
   deployment.check()
   current['epoch']=8
   with self.assertRaisesRegex(RuntimeError,'entry_drain_changed'):deployment.check()
   current['epoch']=7;fence['sourceImageId']='sha256:'+'e'*64
   with self.assertRaisesRegex(RuntimeError,'source_fence_changed'):deployment.check()

if __name__=='__main__':unittest.main()

class LocalMaterialBindingTests(unittest.TestCase):
 def test_local_materials_require_fixed_release_root_and_strict_default_unchanged(self):
  root=Path('/opt/product-development/harbor-local-gateway/local-assurance/fixed-release')
  row={'Mounts':[{'Source':str(root/'pg-data'),'Destination':'/app/data','RW':True},{'Source':str(root/'runtime'),'Destination':'/app/runtime','RW':True}]}
  with patch.object(controller,'local_assurance_module',return_value=SimpleNamespace(RUN=root)):
   self.assertEqual(len(controller.candidate_mounts(row,{'contract':'harbor-local-assurance-deploy-v1'})),2)
   with self.assertRaisesRegex(RuntimeError,'candidate_material_mounts'):controller.candidate_mounts(row)
   row['Mounts'][0]['Source']='/tmp/other'
   with self.assertRaisesRegex(RuntimeError,'candidate_material_mounts'):controller.candidate_mounts(row,{'contract':'harbor-local-assurance-deploy-v1'})
