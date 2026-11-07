import importlib.util
from pathlib import Path
import subprocess
import io
import json
import gzip
import tarfile
import stat
import tempfile
import unittest
from unittest.mock import patch,Mock

spec=importlib.util.spec_from_file_location('guard',Path(__file__).parents[1]/'ci'/'deploy-wrapper.py')
guard=importlib.util.module_from_spec(spec)
spec.loader.exec_module(guard)
CONFIG={'image_prefix':'registry.test/gateway/harbor-local-gateway','port':43127,'origin':'http://gateway.test:43127'}
IMAGE=CONFIG['image_prefix']+':'+'a'*40

class DeploymentGuardTests(unittest.TestCase):
    def test_installed_entry_release_is_explicit_root_only_and_rejects_image_bytes(self):
        config={**CONFIG,'port':8092,'storage_engine':'postgres','availability_policy':'preserve-current','image_transport':'ssh_archive','entry_installed_release':True}
        payload={'action':'deploy-entry-postgres','image':IMAGE}
        with patch.object(guard,'receive_archive') as receive:
            self.assertEqual(guard.receive_deployment_image(io.BytesIO(),payload,config,'deploy-entry-postgres'),(None,'installed'))
            receive.assert_not_called()
            with self.assertRaises(ValueError):guard.receive_deployment_image(io.BytesIO(b'x'),payload,config,'deploy-entry-postgres')
            for change in [{'storage_engine':'sqlite'},{'port':43127},{'availability_policy':'legacy-replace'},{'entry_installed_release':1},{'entry_installed_release':'true'},{'entry_installed_release':None}]:
                with self.subTest(change=change),self.assertRaises(ValueError):guard.receive_deployment_image(io.BytesIO(),payload,{**config,**change},'deploy-entry-postgres')
            with self.assertRaises(ValueError):guard.validate({**payload,'entry_installed_release':True},config)

    def test_installed_entry_release_default_and_old_wire_keep_the_original_transport(self):
        config={**CONFIG,'port':8092,'storage_engine':'postgres','availability_policy':'preserve-current','image_transport':'ssh_archive'}
        payload={'action':'deploy-entry-postgres','image':IMAGE}
        for change,wire in [({},payload),({'entry_installed_release':False},payload),({'entry_installed_release':True},{'action':'deploy','image':IMAGE})]:
            with self.subTest(change=change,wire=wire),patch.object(guard,'receive_archive',return_value='approved-archive') as receive:
                stream=io.BytesIO(b'archive')
                self.assertEqual(guard.receive_deployment_image(stream,wire,{**config,**change},'deploy-entry-postgres'),('approved-archive','ssh_archive'))
                receive.assert_called_once_with(stream,guard.STACK+'/import')

    def test_local_assurance_dispatch_is_root_only_installed_and_does_not_invoke_strict_activation(self):
        config={**CONFIG,'port':8092,'storage_engine':'postgres','local_assurance_release':True,'entry_installed_release':True}
        module=Mock();module.deploy.return_value={'mode':'local-assurance'}
        with patch.object(guard.Path,'lstat',return_value=Mock(st_mode=stat.S_IFREG|0o600,st_uid=0)),patch.object(guard.importlib.util,'spec_from_file_location',return_value=Mock(loader=Mock())) as load,patch.object(guard.importlib.util,'module_from_spec',return_value=module),patch.object(guard,'activation') as activation:
            self.assertEqual(guard.deploy_entry_postgres(IMAGE,config),{'mode':'local-assurance'})
            load.assert_called_once_with('harbor_local_assurance_deploy',Path(guard.LOCAL_ASSURANCE_HELPER))
            self.assertEqual(module.deploy.call_args.args[:3],(IMAGE,config,None));self.assertIs(module.deploy.call_args.args[3],guard.__dict__);activation.assert_not_called()
        for change,archive in [({'local_assurance_release':1},None),({'local_assurance_release':'true'},None),({'local_assurance_release':None},None),({'entry_installed_release':False},None),({},'archive'),({'port':8094},None),({'storage_engine':'sqlite'},None),({'availability_policy':'legacy-replace'},None)]:
            with self.subTest(change=change,archive=archive),patch.object(guard.importlib.util,'spec_from_file_location') as load,self.assertRaises(ValueError):
                guard.deploy_entry_postgres(IMAGE,{**config,**change},archive)
            load.assert_not_called()
        with self.assertRaises(ValueError):guard.validate({'action':'deploy-entry-postgres','image':IMAGE,'local_assurance_release':True},config)

    def test_local_assurance_refuses_untrusted_helper_and_explicit_false_keeps_strict_gate(self):
        config={**CONFIG,'port':8092,'storage_engine':'postgres','local_assurance_release':True,'entry_installed_release':True}
        for info in [Mock(st_mode=stat.S_IFLNK|0o777,st_uid=0),Mock(st_mode=stat.S_IFREG|0o600,st_uid=1000),Mock(st_mode=stat.S_IFREG|0o622,st_uid=0)]:
            with self.subTest(mode=info.st_mode),patch.object(guard.Path,'lstat',return_value=info),patch.object(guard.importlib.util,'spec_from_file_location') as load,self.assertRaises(ValueError):guard.deploy_entry_postgres(IMAGE,config)
            load.assert_not_called()
        with patch.object(guard.Path,'lstat',return_value=Mock(st_mode=stat.S_IFREG|0o600,st_uid=0)),patch.object(guard.importlib.util,'spec_from_file_location',return_value=Mock(loader=Mock())) as load,patch.object(guard.importlib.util,'module_from_spec',return_value=Mock()),patch.object(guard,'activation',side_effect=RuntimeError('strict gate retained')) as activation:
            with self.assertRaisesRegex(RuntimeError,'strict gate retained'):guard.deploy_entry_postgres(IMAGE,{**config,'local_assurance_release':False})
            activation.assert_called_once_with('check',IMAGE)
            self.assertEqual(load.call_args.args[0],'harbor_entry_controller')

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

    def test_entry_action_is_explicit_pg_only_and_never_weakens_legacy_policy(self):
        config={**CONFIG,'port':8092,'storage_engine':'postgres'}
        self.assertEqual(guard.validate({'action':'deploy-entry-postgres','image':IMAGE},config),IMAGE)
        self.assertEqual(guard.deployment_policy(config,'deploy-entry-postgres'),'preserve-current')
        for invalid in [{**config,'storage_engine':'sqlite'},{**config,'port':8082},{**config,'availability_policy':'legacy-replace'}]:
            with self.assertRaises(ValueError):guard.deployment_policy(invalid,'deploy-entry-postgres')
        with self.assertRaises(ValueError):guard.deployment_policy(config,'deploy')

    def test_entry_wrapper_injects_only_controller_probe_and_activates_after_pg_success(self):
        config={**CONFIG,'port':8092,'storage_engine':'postgres'}
        context=Mock(activated=True);controller=Mock();controller.authorize_deployment.return_value=context
        postgres=Mock();postgres.deploy.return_value={'activation':{'state':'active'}}
        approval={'state':'active','sourceFence':{'synthetic':'full'}}
        loader=Mock();spec=Mock(loader=loader)
        with patch.object(guard.Path,'lstat',return_value=Mock(st_mode=stat.S_IFREG|0o600,st_uid=0)),patch.object(guard.importlib.util,'spec_from_file_location',return_value=spec),patch.object(guard.importlib.util,'module_from_spec',side_effect=[controller,postgres]),patch.object(guard,'activation',return_value=approval),patch.object(guard,'docker') as docker:
            result=guard.deploy_entry_postgres(IMAGE,config)
            controller.authorize_deployment.assert_called_once_with(image=IMAGE,approval=approval)
            api=postgres.deploy.call_args.args[3];api['http_code'](8092,'/ready');context.probe.assert_called_once_with(8092,'/ready',None);self.assertIs(api['entry_deployment'],context)
            self.assertEqual(context.check.call_count,2);context.check.assert_called_with(approval);context.activate.assert_called_once_with(approval);self.assertIn('entry',result);docker.assert_not_called()

    def test_entry_controller_rejection_or_final_cas_failure_does_not_call_legacy_deploy(self):
        config={**CONFIG,'port':8092,'storage_engine':'postgres'}
        for phase in ['authorize','activate']:
            with self.subTest(phase=phase):
                context=Mock(activated=False);controller=Mock();controller.authorize_deployment.return_value=context;postgres=Mock();postgres.deploy.return_value={}
                if phase=='authorize':controller.authorize_deployment.side_effect=RuntimeError('source fence refused')
                else:context.activate.side_effect=RuntimeError('CAS refused')
                with patch.object(guard.Path,'lstat',return_value=Mock(st_mode=stat.S_IFREG|0o600,st_uid=0)),patch.object(guard.importlib.util,'spec_from_file_location',return_value=Mock(loader=Mock())),patch.object(guard.importlib.util,'module_from_spec',side_effect=[controller,postgres]),patch.object(guard,'activation',return_value={}),patch.object(guard,'deploy') as legacy,patch.object(guard,'docker') as docker:
                    with self.assertRaises(RuntimeError):guard.deploy_entry_postgres(IMAGE,config)
                    legacy.assert_not_called();docker.assert_not_called()
                    if phase=='authorize':postgres.deploy.assert_not_called()

    def test_root_only_wire_mapping_is_disabled_by_default_and_rejects_invalid_flags(self):
        payload={'action':'deploy','image':IMAGE};config={**CONFIG,'port':8092,'storage_engine':'postgres'}
        missing=object()
        for flag in [missing,None,False,'true',1,{},[]]:
            changed=dict(config)
            if flag is not missing:changed['entry_deployment_enabled']=flag
            with self.subTest(flag=flag),self.assertRaises(ValueError):guard.deployment_action(payload,changed)
        enabled={**config,'entry_deployment_enabled':True};self.assertEqual(guard.deployment_action(payload,enabled),'deploy-entry-postgres')
        for changed in [{**enabled,'port':8082},{**enabled,'storage_engine':'sqlite'},{**enabled,'availability_policy':'legacy-replace'}]:
            with self.assertRaises(ValueError):guard.deployment_action(payload,changed)
        with self.assertRaises(ValueError):guard.validate({**payload,'entry_deployment_enabled':True},enabled)
        with self.assertRaises(ValueError):guard.deployment_policy(enabled,'deploy')
        with patch.object(guard,'docker') as docker,self.assertRaises(ValueError):guard.deploy(IMAGE,enabled)
        docker.assert_not_called()

    def test_strict_controller_four_methods_need_no_activated_attribute_and_probe_adapter_supplies_headers(self):
        class Context:
            __slots__=('events',)
            def __init__(self):self.events=[]
            def check(self,approval=None):self.events.append(('check',approval))
            def probe(self,port,path,headers):self.events.append(('probe',port,path,headers));return 200
            def bind_candidate(self,row):self.events.append(('bind',row))
            def activate(self,approval):self.events.append(('activate',approval));return {'phase':'active','slot':'candidate'}
        context=Context();controller=Mock();controller.authorize_deployment.return_value=context
        def deploy(image,config,archive,api):
            self.assertEqual(api['http_code'](8092,'/ready'),200);api['http_code'](8092,'/health',{'Host':'untrusted.invalid'});return {'activation':{'state':'active'}}
        postgres=Mock();postgres.deploy.side_effect=deploy
        with patch.object(guard.Path,'lstat',return_value=Mock(st_mode=stat.S_IFREG|0o600,st_uid=0)),patch.object(guard.importlib.util,'spec_from_file_location',return_value=Mock(loader=Mock())),patch.object(guard.importlib.util,'module_from_spec',side_effect=[controller,postgres]),patch.object(guard,'activation',return_value={'state':'active'}):
            result=guard.deploy_entry_postgres(IMAGE,{**CONFIG,'port':8092,'storage_engine':'postgres'})
            self.assertEqual(result['entry']['slot'],'candidate');self.assertIn(('probe',8092,'/ready',None),context.events)

    def test_main_root_wire_mapping_still_calls_full_entry_gate_and_cannot_fall_back(self):
        from unittest.mock import mock_open
        cases=[('deploy',{'entry_deployment_enabled':True}),('deploy-entry-postgres',{'entry_installed_release':True,'image_transport':'ssh_archive'})]
        for wire,extra in cases:
            config={**CONFIG,'port':8092,'storage_engine':'postgres',**extra}
            with self.subTest(wire=wire),tempfile.TemporaryDirectory() as directory:
                target=Path(directory)/'config';target.write_text(json.dumps(config));stdin=Mock(buffer=io.BytesIO((json.dumps({'action':wire,'image':IMAGE})+'\n').encode()))
                with patch.object(guard,'CONFIG',str(target)),patch.object(guard.sys,'stdin',stdin),patch.object(guard.Path,'lstat',return_value=Mock(st_mode=stat.S_IFREG|0o600,st_uid=0)),patch.object(guard,'open',mock_open(),create=True),patch.object(guard.fcntl,'flock'),patch.object(guard,'receive_archive') as receive,patch.object(guard,'deploy_entry_postgres',side_effect=RuntimeError('controller gate refused')) as entry,patch.object(guard,'deploy') as legacy:
                    with self.assertRaisesRegex(RuntimeError,'controller gate refused'):guard.main()
                    entry.assert_called_once_with(IMAGE,config,None);legacy.assert_not_called();receive.assert_not_called()

if __name__=='__main__':unittest.main()
