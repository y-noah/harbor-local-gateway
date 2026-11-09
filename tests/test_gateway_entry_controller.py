import importlib.util
import json
import copy
import unittest
import stat
import io
from contextlib import redirect_stdout
from types import SimpleNamespace
from pathlib import Path
from unittest.mock import Mock,patch

ROOT=Path(__file__).resolve().parents[1]
def load(name,path):
 spec=importlib.util.spec_from_file_location(name,path);module=importlib.util.module_from_spec(spec);spec.loader.exec_module(module);return module
controller=load('gateway_entry_controller',ROOT/'ci/gateway-entry-controller.py')
bridge=load('gateway_legacy_transparent',ROOT/'ci/gateway-legacy-transparent.py')

class EntryBoundaryTests(unittest.TestCase):
 def test_idle_wait_only_reads_active_entry_and_returns_the_idle_epoch(self):
  states=[{'phase':'active','slot':'candidate','inflight':n,'epoch':7} for n in [2,1,0]]
  with patch.object(controller,'private_json',return_value={'drainTimeoutMs':100,'shutdownTimeoutMs':1}),patch.object(controller,'entry_state',side_effect=states) as query,patch.object(controller.time,'monotonic',return_value=0),patch.object(controller.time,'sleep') as sleep:
   self.assertEqual(controller.wait_entry_idle(),states[-1]);self.assertEqual(query.call_args_list,[unittest.mock.call(timeout=0.1)]*3);self.assertEqual(sleep.call_count,2)

 def test_idle_wait_timeout_and_changed_phase_never_issue_control_mutation(self):
  active={'phase':'active','slot':'candidate','inflight':1,'epoch':7}
  with patch.object(controller,'private_json',return_value={'drainTimeoutMs':10}),patch.object(controller,'entry_state',return_value=active) as query,patch.object(controller.time,'monotonic',side_effect=[0,0,0.010]),patch.object(controller.time,'sleep') as sleep:
   with self.assertRaisesRegex(RuntimeError,'entry_idle_timeout'):controller.wait_entry_idle()
   query.assert_called_once_with(timeout=0.010);sleep.assert_not_called()
  with patch.object(controller,'private_json',return_value={'shutdownTimeoutMs':100}),patch.object(controller,'entry_state',side_effect=[active,{**active,'phase':'holding'}]) as query,patch.object(controller.time,'monotonic',return_value=0),patch.object(controller.time,'sleep'):
   with self.assertRaisesRegex(RuntimeError,'entry_idle_state'):controller.wait_entry_idle()
   self.assertEqual(query.call_args_list,[unittest.mock.call(timeout=0.1)]*2)

 def test_idle_wait_does_not_accept_late_idle_or_query_again_after_sleep_deadline(self):
  for states,clock in [([{'phase':'active','slot':'candidate','inflight':0}],[0,0,2]),([{'phase':'active','slot':'candidate','inflight':1}],[0,0,0,0.020])]:
   with self.subTest(clock=clock),patch.object(controller,'private_json',return_value={'drainTimeoutMs':10}),patch.object(controller,'entry_state',side_effect=states) as query,patch.object(controller.time,'monotonic',side_effect=clock),patch.object(controller.time,'sleep'):
    with self.assertRaisesRegex(RuntimeError,'entry_idle_timeout'):controller.wait_entry_idle()
    query.assert_called_once_with(timeout=0.010)
  with patch.object(controller,'private_json',return_value={'drainTimeoutMs':10}),patch.object(controller,'entry_state',side_effect=TimeoutError),patch.object(controller.time,'monotonic',return_value=0):
   with self.assertRaisesRegex(RuntimeError,'entry_idle_timeout'):controller.wait_entry_idle()

 def test_control_query_timeout_rejects_invalid_values_before_opening_socket(self):
  for timeout in [None,True,False,0,-1,'1',float('inf'),float('nan'),126]:
   with self.subTest(timeout=timeout),patch.object(controller,'UnixHttp') as client:
    with self.assertRaisesRegex(RuntimeError,'entry_control_timeout'):controller.entry_state(timeout=timeout)
    client.assert_not_called()

 def test_idle_wait_rejects_invalid_budget_and_unknown_inflight(self):
  for budget in [None,True,False,0,-1,'100',1.0,2147483648]:
   with self.subTest(budget=budget),patch.object(controller,'private_json',return_value={'drainTimeoutMs':budget}),patch.object(controller,'entry_state') as query:
    with self.assertRaisesRegex(RuntimeError,'entry_idle_budget'):controller.wait_entry_idle()
    query.assert_not_called()
  for count in [None,True,-1,'0']:
   with self.subTest(count=count),patch.object(controller,'private_json',return_value={'shutdownTimeoutMs':100}),patch.object(controller,'entry_state',return_value={'phase':'active','slot':'candidate','inflight':count}):
    with self.assertRaisesRegex(RuntimeError,'entry_idle_count'):controller.wait_entry_idle()

 def control_reply(self,status,body):
  response=Mock(status=status);response.read.return_value=body
  client=Mock();client.getresponse.return_value=response
  trusted=SimpleNamespace(st_mode=stat.S_IFSOCK|0o600,st_uid=0)
  with patch.object(Path,'lstat',return_value=trusted),patch.object(controller,'trusted_parents'),patch.object(controller,'UnixHttp',return_value=client):
   try:return controller.entry_state('drain',3)
   finally:client.close.assert_called_once();response.read.assert_called_once_with(65537)

 def test_control_failure_preserves_safe_drain_evidence_without_response_contents(self):
  payload={'error':{'code':'gateway_entry_drain_timeout','message':'private-body'},'phase':'holding','slot':'candidate','epoch':3,'inflight':2,'queued':19,'private':'private-body'}
  with self.assertRaisesRegex(RuntimeError,'^entry_controller_entry_control_failed$') as caught:
   self.control_reply(409,json.dumps(payload).encode())
  self.assertEqual(caught.exception.diagnostic,{'reason':'gateway_entry_drain_timeout','phase':'holding','slot':'candidate','epoch':3,'inflight':2,'queued':19})
  self.assertNotIn('private-body',str(caught.exception.__cause__))
  output=io.StringIO()
  def fail():raise caught.exception
  with redirect_stdout(output),self.assertRaises(SystemExit) as exit:controller.run_cli(fail)
  self.assertEqual(exit.exception.code,1);saved=json.loads(output.getvalue())
  self.assertEqual(saved,{'error':{'code':'entry_controller_entry_control_failed','diagnostic':caught.exception.diagnostic},'sourceStopped':False,'automaticSqliteRollback':False})
  self.assertNotIn('private-body',output.getvalue())

 def test_cli_other_failures_keep_their_fixed_error_contract_and_no_arbitrary_text(self):
  for error,expected in [(RuntimeError('entry_controller_control_untrusted'),'entry_controller_control_untrusted'),(RuntimeError('private-body'),'entry_controller_failed_closed'),(ValueError('private-body'),'entry_controller_failed_closed')]:
   output=io.StringIO()
   def fail():raise error
   with self.subTest(expected=expected),redirect_stdout(output),self.assertRaises(SystemExit) as exit:controller.run_cli(fail)
   self.assertEqual(exit.exception.code,1);self.assertEqual(json.loads(output.getvalue())['error'],{'code':expected});self.assertNotIn('private-body',output.getvalue())

 def test_control_rejects_malformed_or_untrusted_diagnostics_without_leaking_them(self):
  cases=[b'private-body',b'[]',b'null',b'\xff',json.dumps({'error':{'code':['private-body']},'phase':{},'slot':'private-body','epoch':True,'inflight':-1,'queued':2**54}).encode(),json.dumps({'error':{'code':'private-body'}}).encode(),b'x'*65537]
  for body in cases:
   with self.subTest(bodyLength=len(body)),self.assertRaisesRegex(RuntimeError,'^entry_controller_entry_control_failed$'):
    self.control_reply(409,body)

 def test_control_success_and_success_body_budget_keep_existing_contract(self):
  self.assertEqual(self.control_reply(200,b'{"phase":"drained","epoch":4}'),{'phase':'drained','epoch':4})
  with self.assertRaisesRegex(RuntimeError,'^entry_controller_entry_control_failed$'):self.control_reply(200,b'x'*65537)

 def test_deep_failure_json_retains_control_error_at_real_cli_boundary(self):
  body=b'['*1100+b'"private-body"'+b']'*1100;output=io.StringIO()
  with redirect_stdout(output),self.assertRaises(SystemExit) as exit:controller.run_cli(lambda:self.control_reply(409,body))
  self.assertEqual(exit.exception.code,1)
  self.assertEqual(json.loads(output.getvalue())['error'],{'code':'entry_controller_entry_control_failed'})
  self.assertNotIn('private-body',output.getvalue())

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

class DynamicSourcePeerTests(unittest.TestCase):
 def config(self):return {'source':{'containerId':'source'},'bridge':{'ports':['source-veth','old-peer']}}
 def policy(self):return {'allowDynamicBridgePeers':True,'source':{'containerId':'source'},'sourceBridgePort':'source-veth','dynamicPeerDeviceIndices':{'source-veth':10,'old-peer':20}}
 def test_background_peer_churn_allowed_but_source_missing_or_reused_rejected(self):
  config=self.config();policy=self.policy()
  with patch.object(controller,'dynamic_bridge_policy',return_value=policy),patch.object(controller,'verify_source_bridge_owners'):
   controller.verify_bridge_peers(config,[{'ifname':'source-veth','ifindex':10},{'ifname':'new-peer','ifindex':30}])
   for ports in [[{'ifname':'new-peer','ifindex':30}],[{'ifname':'source-veth','ifindex':99}]]:
    with self.assertRaisesRegex(RuntimeError,'critical_bridge_port_changed'):controller.verify_bridge_peers(config,ports)
 def test_strict_default_rejects_same_background_change(self):
  with patch.object(controller,'dynamic_bridge_policy',return_value=None),self.assertRaisesRegex(RuntimeError,'bridge_ports_changed'):controller.verify_bridge_peers(self.config(),[{'ifname':'source-veth','ifindex':10},{'ifname':'new-peer','ifindex':30}])
 def test_disappeared_background_tc_skipped_and_live_original_tc_still_checked(self):
  policy={**self.policy(),'phase':'prepared','rulesDigest':'rules','mangleDigest':'mangle','tcDigests':{'source-veth':'a','old-peer':'b'}}
  def execute(args):return json.dumps({'nftables':[{'set':{'elem':[]}}]}) if args[0]=='/usr/sbin/nft' else json.dumps([{'ifname':'source-veth','ifindex':10},{'ifname':'new-peer','ifindex':30}])
  with patch.object(controller,'dynamic_bridge_policy',return_value=policy),patch.object(controller,'state_rules_digest',return_value='rules'),patch.object(controller,'mangle_digest',return_value='mangle'),patch.object(controller,'execute',side_effect=execute),patch.object(controller,'tc_digest',return_value='a') as tc:
   controller.verify_resources(policy,self.config());tc.assert_called_once_with('source-veth')
   policy['tcDigests']['source-veth']='changed'
   with self.assertRaisesRegex(RuntimeError,'tc_rules_changed'):controller.verify_resources(policy,self.config())
 def test_candidate_missing_or_name_collision_never_ignored(self):
  for which in ['candidate','candidate-collision']:
   policy={**self.policy(),'phase':'prepared','rulesDigest':'rules','mangleDigest':'mangle','tcDigests':{'old-peer':'b'} if which=='candidate-collision' else {'candidate-veth':'b'},'candidateDeviceIndices':{'candidate-veth':40,'old-peer':40}}
   def execute(args):return json.dumps({'nftables':[{'set':{'elem':[]}}]}) if args[0]=='/usr/sbin/nft' else json.dumps([{'ifname':'old-peer','ifindex':99}])
   with patch.object(controller,'dynamic_bridge_policy',return_value=policy),patch.object(controller,'state_rules_digest',return_value='rules'),patch.object(controller,'mangle_digest',return_value='mangle'),patch.object(controller,'execute',side_effect=execute),self.assertRaisesRegex(RuntimeError,'tc_device_identity_changed'):controller.verify_resources(policy,self.config())
 def test_candidate_bridge_membership_remains_exact(self):
  state={'candidateNetworkIdentity':{'id':'net','name':'approved','bridge':'br-own','ifindex':4,'mac':'aa','ports':{'own':5}},'candidateNetworkContainerIds':['pg','gw','worker']}
  responses=[json.dumps([{'ifindex':4,'address':'aa'}]),json.dumps([{'ifname':'own','ifindex':5},{'ifname':'foreign','ifindex':8}])]
  with patch.object(controller,'execute',side_effect=responses),self.assertRaisesRegex(RuntimeError,'candidate_bridge_ports_changed'):controller.verify_candidate_bridge(state)

class DynamicOwnInventoryTests(unittest.TestCase):
 def test_unexpected_harbor_owned_peer_on_source_network_is_not_background(self):
  cid='c'*64;row={'Config':{'Labels':{'com.aistrong.project':'harbor-local-gateway'}},'NetworkSettings':{'Networks':{'bridge':{'NetworkID':'source-network','IPAddress':'172.17.0.9'}}}}
  policy={'dynamicBridgeIdentity':{'networkId':'source-network'}}
  with patch.object(controller,'execute',return_value=cid+'\n'),patch.object(controller,'inspect_container',return_value=row):
   with self.assertRaisesRegex(RuntimeError,'unexpected_harbor_source_bridge_peer'):controller.verify_source_bridge_owners({'source':{'containerId':'different'}},policy)
   controller.verify_source_bridge_owners({'source':{'containerId':cid}},policy)

class DynamicPeerIdentityTransitionTests(unittest.TestCase):
 def test_recreated_foreign_interface_is_background_but_same_identity_tc_is_pinned(self):
  policy={'allowDynamicBridgePeers':True,'phase':'prepared','sourceBridgePort':'source','rulesDigest':'r','mangleDigest':'m','tcDigests':{'peer':'expected'},'dynamicPeerDeviceIndices':{'peer':20}}
  live=[{'ifname':'peer','ifindex':99}]
  def execute(args):return json.dumps({'nftables':[{'set':{'elem':[]}}]}) if args[0]=='/usr/sbin/nft' else json.dumps(live)
  with patch.object(controller,'dynamic_bridge_policy',return_value=policy),patch.object(controller,'state_rules_digest',return_value='r'),patch.object(controller,'mangle_digest',return_value='m'),patch.object(controller,'execute',side_effect=execute),patch.object(controller,'tc_digest',return_value='changed') as tc:
   controller.verify_resources(policy,{});tc.assert_not_called()
   live[0]['ifindex']=20
   with self.assertRaisesRegex(RuntimeError,'tc_rules_changed'):controller.verify_resources(policy,{})

class DynamicPeerInitialAbsenceTests(unittest.TestCase):
 def test_peer_already_absent_at_policy_capture_does_not_become_critical(self):
  policy={'allowDynamicBridgePeers':True,'phase':'prepared','sourceBridgePort':'source','rulesDigest':'r','mangleDigest':'m','tcDigests':{'old-peer':'old'},'dynamicPeerDeviceIndices':{'old-peer':None}}
  def execute(args):return json.dumps({'nftables':[{'set':{'elem':[]}}]}) if args[0]=='/usr/sbin/nft' else '[]'
  with patch.object(controller,'dynamic_bridge_policy',return_value=policy),patch.object(controller,'state_rules_digest',return_value='r'),patch.object(controller,'mangle_digest',return_value='m'),patch.object(controller,'execute',side_effect=execute),patch.object(controller,'tc_digest') as tc:
   controller.verify_resources(policy,{});tc.assert_not_called()
   policy['candidateDeviceIndices']={'old-peer':None}
   with self.assertRaisesRegex(RuntimeError,'tc_device_identity_changed'):controller.verify_resources(policy,{})
