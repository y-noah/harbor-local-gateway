import copy
import importlib.util
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

ROOT=Path(__file__).resolve().parents[1]
def load(name,path):
 spec=importlib.util.spec_from_file_location(name,path);module=importlib.util.module_from_spec(spec);spec.loader.exec_module(module);return module
recovery=load('source_recovery',ROOT/'ci/gateway-source-recovery.py')
controller=load('entry_controller',ROOT/'ci/gateway-entry-controller.py')
inventory=load('reviewed_container_inventory',ROOT/'ci/storage-recovery/rotate-tools.py')

class RecoveryBoundaryTests(unittest.TestCase):
 def test_any_durable_writer_or_candidate_authorization_blocks_sqlite(self):
  with tempfile.TemporaryDirectory() as directory:
   class Boundary:
    require=staticmethod(controller.require)
    ACTIVE=Path(directory)/'active'
    WRITER_AUTHORIZED=Path(directory)/'writer'
   for path in [Boundary.ACTIVE,Boundary.WRITER_AUTHORIZED]:
    path.touch()
    with self.assertRaises(RuntimeError):recovery.no_writer(Boundary,{})
    path.unlink()
   for field in ['writerAuthorized','candidate','candidateIntent','candidateOperations','activeRuntimeConfig']:
    with self.subTest(field=field),self.assertRaises(RuntimeError):recovery.no_writer(Boundary,{field:True})
   recovery.no_writer(Boundary,{'deploymentBinding':{'pending':'preconsume'}})

 def test_full_inventory_rejects_unlabelled_writer_and_historical_candidate(self):
  class Boundary:
   require=staticmethod(controller.require)
   rows=[]
   @classmethod
   def local_container_inventory(cls):return cls.rows
  for row in [{'Id':'other','Name':'/harbor-local-gateway-worker','Config':{},'State':{'Running':False}}, {'Id':'other','Name':'/removed-candidate','Config':{'Labels':{'com.aistrong.project':'harbor-local-gateway','com.harbor.activation-deployment-id':'old'}},'State':{'Running':False}}]:
   Boundary.rows=[row]
   with patch.object(recovery,'inventory_module',return_value=inventory),self.assertRaises(Exception):recovery.no_candidates(Boundary,{'containerId':'source'})

 def test_nonrunning_other_project_container_requires_positive_resource_release(self):
  class Boundary:
   require=staticmethod(controller.require)
   rows=[]
   @classmethod
   def local_container_inventory(cls):return cls.rows
  base={'Status':'exited','Pid':0,'Running':False,'Paused':False,'Restarting':False,'Dead':False}
  with patch.object(recovery,'inventory_module',return_value=inventory):
   for state in [base,{**base,'Status':'created'}]:
    Boundary.rows=[{'Id':'other','Name':'/archive','Config':{'Labels':{'com.aistrong.project':'harbor-local-gateway'}},'State':state}];recovery.no_candidates(Boundary,{'containerId':'source'})
   for state in [{**base,'Status':'removing'},{**base,'Status':'dead'},{**base,'Pid':123},{**base,'Dead':True},{**base,'Paused':True},{**base,'Restarting':True},{**base,'Status':'unknown'},{**base,'Pid':None},{key:value for key,value in base.items() if key!='Running'}]:
    Boundary.rows=[{'Id':'other','Name':'/archive','Config':{'Labels':{'com.aistrong.project':'harbor-local-gateway'}},'State':state}]
    with self.subTest(state=state),self.assertRaises(Exception):recovery.no_candidates(Boundary,{'containerId':'source'})

 def test_proof_requires_actual_fence_source_mount_roles_and_preconsume(self):
  class Boundary:require=staticmethod(controller.require)
  source={'containerId':'a'*64,'imageId':'sha256:'+'b'*64,'startedAt':'old-generation'}
  row={'State':{'FinishedAt':'finished'},'Mounts':[{'Type':'bind','Source':'/tmp','Destination':'/app/data','RW':True}]}
  proof={'contract':'harbor-source-recovery-v1','runId':'12345678-1234-4234-8234-123456789abc','configurationDigest':'a'*64,'sourceFence':{'scope':'docker-systemd','sourceContainerId':source['containerId'],'sourceImageId':source['imageId'],'sourceStartedAt':source['startedAt'],'sourceFinishedAt':'finished','sourceStopped':True,'supervisorFenced':True,'productionVerified':True},'sourceDataDir':'/tmp','supervisorKind':'docker-restart','activationState':'pending','deploymentActivationAbsent':True,'rolesNoLogin':True,'roleStates':{'migration':'NOLOGIN','owner':'NOLOGIN','runtime':'NOLOGIN'},'targetSessionCount':0,'sourceVerified':True,'sourceVerificationMode':'authenticated-captured-source-match','registryVerified':True,'inventoryVerified':True}
  recovery.validate_proof(Boundary,proof,{'source':source},row)
  baseline=copy.deepcopy(proof);baseline['activationState']='not-created';baseline['roleStates']={key:'not-created' for key in ['migration','owner','runtime']};recovery.validate_proof(Boundary,baseline,{'source':source},row)
  for field,value in [('contract','harbor-source-recovery-diagnostic'),('supervisorKind','systemd'),('activationState','active'),('deploymentActivationAbsent',False),('targetSessionCount',1),('sourceDataDir','/other'),('registryVerified',False),('sourceVerified',False),('roleStates',{'migration':'NOLOGIN','owner':'NOLOGIN','runtime':'LOGIN'})]:
   changed=copy.deepcopy(proof);changed[field]=value
   with self.subTest(field=field),self.assertRaises(RuntimeError):recovery.validate_proof(Boundary,changed,{'source':source},row)
  proof['sourceFence']['sourceStartedAt']='another-generation'
  with self.assertRaises(RuntimeError):recovery.validate_proof(Boundary,proof,{'source':source},row)

if __name__=='__main__':unittest.main()
