import importlib.util
from pathlib import Path
import subprocess
import io
import json
import gzip
import tarfile
import tempfile
import unittest
from unittest.mock import patch,Mock

spec=importlib.util.spec_from_file_location('guard',Path(__file__).parents[1]/'ci'/'deploy-wrapper.py')
guard=importlib.util.module_from_spec(spec)
spec.loader.exec_module(guard)
CONFIG={'image_prefix':'registry.test/gateway/harbor-local-gateway','port':43127,'origin':'http://gateway.test:43127'}
IMAGE=CONFIG['image_prefix']+':'+'a'*40

class DeploymentGuardTests(unittest.TestCase):
    def archive(self,directory,image=IMAGE,extra=None):
        path=Path(directory)/'image.tar'
        content={'manifest.json':json.dumps([{'RepoTags':[image],'Config':'config.json','Layers':['layer.tar']}]).encode(),
                 'config.json':b'{}','layer.tar':b'layer',
                 'repositories':json.dumps({image.rsplit(':',1)[0]:{image.rsplit(':',1)[1]:'layer'}}).encode()}
        content.update(extra or {})
        with tarfile.open(path,'w') as archive:
            for name,body in content.items():
                member=tarfile.TarInfo(name);member.size=len(body)
                archive.addfile(member,io.BytesIO(body))
        return path

    def test_archive_allows_exact_image_and_rejects_foreign_tags_and_paths(self):
        with tempfile.TemporaryDirectory() as directory:
            guard.validate_archive(self.archive(directory),IMAGE)
            for image,extra in [('registry.test/other:latest',None),(IMAGE,{'../outside':b'x'}),
                                (IMAGE,{'other-image.json':b'{}'}),
                                (IMAGE,{'repositories':b'{"other":{"latest":"layer"}}'}),
                                (IMAGE,{'manifest.json':b'[{},{}]'})]:
                with self.subTest(image=image,extra=extra),self.assertRaises(ValueError):
                    guard.validate_archive(self.archive(directory,image,extra),IMAGE)
            path=self.archive(directory)
            with tarfile.open(path,'a') as archive:
                link=tarfile.TarInfo('external');link.type=tarfile.SYMTYPE;link.linkname='/etc/shadow';archive.addfile(link)
            with self.assertRaises(ValueError):guard.validate_archive(path,IMAGE)

    def test_received_archive_is_bounded_and_cleaned_on_failure(self):
        with tempfile.TemporaryDirectory() as directory:
            stream=io.BytesIO(gzip.compress(b'image'))
            with patch.object(guard.shutil,'disk_usage',return_value=Mock(free=10*1024**3)):
                path=guard.receive_archive(stream,directory)
                self.assertEqual(Path(path).read_bytes(),b'image');Path(path).unlink()
                with patch.object(guard,'MAX_ARCHIVE_BYTES',4),self.assertRaises(ValueError):
                    guard.receive_archive(io.BytesIO(gzip.compress(b'image')),directory)
            self.assertEqual(list(Path(directory).iterdir()),[])

    def test_invalid_archive_never_reaches_docker_load(self):
        with tempfile.TemporaryDirectory() as directory:
            path=self.archive(directory,'registry.test/other:latest')
            with patch.object(guard,'inspect',side_effect=[None,None]),patch.object(guard,'run',return_value=Mock(stdout='')),patch.object(guard,'docker') as docker:
                with self.assertRaises(ValueError):guard.deploy(IMAGE,CONFIG,path)
                docker.assert_not_called()

    def test_only_service_images_are_allowed(self):
        self.assertEqual(guard.validate({'action':'deploy','image':IMAGE},CONFIG),IMAGE)
        invalid=[{},[],{'action':'admin-password'},{'action':'shell','image':IMAGE},{'action':'deploy','image':IMAGE,'mount':'/'},
                 {'action':'admin-password','path':'/etc/shadow'},
                 {'action':'deploy','image':'registry.test/other/service:'+'a'*40},
                 {'action':'deploy','image':CONFIG['image_prefix']+':latest'},
                 {'action':'deploy','image':IMAGE+';id'}]
        for request in invalid:
            with self.subTest(request=request),self.assertRaises(ValueError):
                guard.validate(request,CONFIG)

    def test_failed_update_restores_previous_container(self):
        current={'Config':{'Labels':{'com.aistrong.project':guard.NAME}}}
        calls=[]
        def docker(*args,**kwargs):
            calls.append(args)
            if args[0]=='run':raise subprocess.CalledProcessError(1,['docker'])
        with patch.object(guard,'inspect',side_effect=[current,None]),patch.object(guard,'run',return_value=Mock(stdout='')),patch.object(guard,'docker',side_effect=docker),patch.object(guard.Path,'mkdir'),patch.object(guard.os,'chown'):
            with self.assertRaises(subprocess.CalledProcessError):guard.deploy(IMAGE,CONFIG)
        self.assertIn(('rename',guard.NAME,guard.NAME+'-previous'),calls)
        self.assertIn(('rm','-f',guard.NAME),calls)
        self.assertIn(('rename',guard.NAME+'-previous',guard.NAME),calls)
        self.assertEqual(calls[-1],('start',guard.NAME))

    def test_contract_rejects_old_candidate_before_stopping_service(self):
        config={**CONFIG,'required_data_contract':'session-metadata-only-v1'}
        current={'Config':{'Labels':{'com.aistrong.project':guard.NAME}}}
        with patch.object(guard,'inspect',side_effect=[current,None,{'Config':{'Labels':{}}}]),patch.object(guard,'run',return_value=Mock(stdout='')),patch.object(guard,'docker') as docker:
            with self.assertRaisesRegex(ValueError,'data contract'):guard.deploy(IMAGE,config)
            self.assertNotIn(('stop','-t','30',guard.NAME),[call.args for call in docker.call_args_list])

    def test_health_failure_restores_only_contract_compatible_previous_service(self):
        contract='session-metadata-only-v1';config={**CONFIG,'required_data_contract':contract}
        candidate={'Config':{'Image':IMAGE,'Labels':{'com.harbor.data-contract':contract}},'State':{'Health':{'Status':'healthy'}}}
        for compatible in (False,True):
            labels={'com.aistrong.project':guard.NAME}
            if compatible:labels['com.harbor.data-contract']=contract
            current={'Config':{'Labels':labels}}
            with self.subTest(compatible=compatible),patch.object(guard,'inspect',side_effect=[current,None,candidate,candidate]),patch.object(guard,'run',return_value=Mock(stdout='')),patch.object(guard,'docker') as docker,patch.object(guard.Path,'mkdir'),patch.object(guard.os,'chown'),patch.object(guard,'http_code',side_effect=[200,200]):
                with self.assertRaisesRegex(RuntimeError,'authentication'):guard.deploy(IMAGE,config)
                calls=[call.args for call in docker.call_args_list]
                self.assertIn(('run','-d','--name',guard.NAME),[args[:4] for args in calls if args[0]=='run'])
                self.assertEqual(('start',guard.NAME) in calls,compatible)
                self.assertIn(('rename',guard.NAME+'-previous',guard.NAME),calls)

    def test_contract_configuration_is_validated_before_docker_access(self):
        for value in ['',False,[],{},'Bad Contract']:
            with self.subTest(value=value),patch.object(guard,'docker') as docker:
                with self.assertRaises(ValueError):guard.deploy(IMAGE,{**CONFIG,'required_data_contract':value})
                docker.assert_not_called()

    def test_other_service_and_occupied_port_are_not_modified(self):
        for current,ports in [({'Config':{'Labels':{'com.aistrong.project':'other'}}},''),(None,'LISTEN'),({'Config':{'Labels':{'com.aistrong.project':guard.NAME}},'HostConfig':{'PortBindings':{'43127/tcp':[{'HostPort':'43128'}]}}},'LISTEN')]:
            with patch.object(guard,'inspect',side_effect=[current,None]),patch.object(guard,'docker') as docker,patch.object(guard,'run',return_value=Mock(stdout=ports)):
                with self.assertRaises(ValueError):guard.deploy(IMAGE,CONFIG)
                docker.assert_not_called()

if __name__=='__main__':unittest.main()
