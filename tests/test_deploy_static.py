import base64
import importlib.util
import io
import json
import os
from pathlib import Path
import tempfile
import threading
import unittest
from http.server import BaseHTTPRequestHandler,ThreadingHTTPServer
from unittest.mock import Mock,patch

ROOT=Path(__file__).parents[1]
def load(name,file):
    spec=importlib.util.spec_from_file_location(name,ROOT/'ci'/file);module=importlib.util.module_from_spec(spec);spec.loader.exec_module(module);return module
release=load('static_release','static-release.py');guard=load('static_guard','deploy-wrapper.py');builder=load('static_builder','static-release-build.py');prepare=load('static_prepare','static-release-prepare.py')
OLD={'index.html':b'<html><body>old</body></html>','app.js':b'const oldPage=true;','style.css':b'body{color:rgb(1,2,3)}'}
NEW={'index.html':b'<html data-harbor-static-revision="new"><body>new</body></html>','app.js':b'const compatibleBootstrap=true;'}
def approved():
    return {'version':1,'revision':'b'*40,'bundle_sha256':'d'*64,'backend':{'source_revision':'a'*40,'image_id':'sha256:'+'c'*64},'before':release.hashes(OLD),'after':release.hashes({**OLD,**NEW})}

class StaticReleaseTests(unittest.TestCase):
    def directory(self,path,files):
        path.mkdir()
        for name,data in files.items():(path/name).write_bytes(data)
        return os.open(path,os.O_RDONLY|os.O_DIRECTORY)
    def test_8092_default_and_postgres_old_jobs_cannot_reach_any_docker_operation(self):
        for config in [{'port':8092},{'port':8092,'storage_engine':'postgres'},{'port':8092,'availability_policy':'legacy-replace'},{'port':8082,'availability_policy':'preserve-current'}]:
            with self.subTest(config=config),patch.object(guard,'docker') as docker,patch.object(guard,'inspect') as inspect:
                with self.assertRaises(ValueError):guard.deploy('immutable-but-prohibited',config)
                docker.assert_not_called();inspect.assert_not_called()
        self.assertEqual(guard.deployment_policy({'port':8082},'deploy'),'legacy-replace')
        self.assertEqual(guard.deployment_policy({'port':8092},'deploy-static'),'preserve-current')
        with self.assertRaises(ValueError):guard.deployment_policy({'port':8092,'availability_policy':'client-override'},'deploy-static')
        with self.assertRaises(ValueError):guard.deployment_policy({'port':8092},'handoff')
    def test_root_port_drift_does_not_stop_an_existing_8092_container(self):
        current={'HostConfig':{'PortBindings':{'43127/tcp':[{'HostPort':'8092'}]}}}
        with patch.object(guard,'inspect',return_value=current),patch.object(guard,'docker') as docker:
            with self.assertRaisesRegex(ValueError,'8092'):guard.deploy('unused',{'port':8082})
            docker.assert_not_called()
    def test_compile_preserves_original_sources_without_eval_inline_or_data_modules(self):
        sources=[{'revision':rev,'files':{name:builder.encode(data) for name,data in OLD.items()}} for rev in ['a'*40,'b'*40]]
        text=builder.compile_app(sources,'a'*40).decode()
        self.assertEqual(text.count(OLD['app.js'].decode()),2);self.assertIn('new CSSStyleSheet()',text);self.assertNotIn('eval(',text);self.assertNotIn('data:text',text)
        self.assertEqual(builder.html_for(OLD['index.html'],'a'*40,'a'*40),OLD['index.html'])
        self.assertIn(b'data-harbor-static-revision="'+b'b'*40+b'"',builder.html_for(OLD['index.html'],'b'*40,'a'*40))
        for source in [b'import x from "./other.js";',b'export const x=1;',b'console.log(import.meta.url);']:
            sources[1]['files']['app.js']=builder.encode(source)
            with self.assertRaises(ValueError):builder.compile_app(sources,'a'*40)
        sources[1]['files']['app.js']=builder.encode(OLD['app.js']);sources[1]['files']['style.css']=builder.encode(b'@import "/other.css";')
        with self.assertRaises(ValueError):builder.compile_app(sources,'a'*40)
    def test_continuous_releases_inherit_all_html_branches_and_logical_rollback_index(self):
        a_files={**OLD,'app.js':b'const branchA=true;'};b_files={**OLD,'app.js':b'const branchB=true;'}
        with patch.object(builder,'git_files',side_effect=[OLD,a_files]):a=builder.build('b'*40,'a'*40)
        with patch.object(builder,'git_files',side_effect=[OLD,b_files]):b=builder.build('c'*40,'a'*40,a)
        self.assertEqual([row['revision'] for row in b['sources']],['a'*40,'b'*40,'c'*40]);self.assertEqual(b['before'],a['after'])
        app=builder.decode(b['assets']['app.js']);self.assertIn(b'const branchA=true;',app);self.assertIn(b'const branchB=true;',app)
        with patch.object(builder,'git_files',side_effect=[OLD,b_files]):rollback=builder.build('c'*40,'a'*40,a,previous_index_revision='a'*40)
        self.assertEqual(rollback['before']['index.html'],release.digest(OLD['index.html']));self.assertEqual(rollback['before']['app.js'],a['after']['app.js'])
    def test_previous_ci_artifact_requires_same_origin_exact_digest_and_qualification(self):
        value={'compatibility':{'passed':True}};raw=json.dumps(value).encode();env={'STATIC_PREVIOUS_JOB_ID':'123','STATIC_PREVIOUS_BUNDLE_SHA256':release.digest(raw),'CI_PROJECT_ID':'456','CI_API_V4_URL':'http://gitlab.test/api/v4','CI_PROJECT_URL':'http://gitlab.test/group/project','CI_JOB_TOKEN':'synthetic-ci-token'}
        response=io.BytesIO(raw);response.status=200;opener=Mock();opener.open.return_value=response
        with patch.object(prepare.urllib.request,'build_opener',return_value=opener):self.assertEqual(prepare.previous_bundle(env),value)
        request=opener.open.call_args.args[0];self.assertEqual(request.full_url,'http://gitlab.test/api/v4/projects/456/jobs/123/artifacts/.ci-static/release.json')
        self.assertEqual(prepare.previous_bundle({}),None)
        for bad in [{**env,'CI_API_V4_URL':'http://foreign.test/api/v4'},{**env,'STATIC_PREVIOUS_JOB_ID':'../123'},{**env,'STATIC_PREVIOUS_BUNDLE_SHA256':''}]:
            with self.assertRaises(ValueError):prepare.previous_bundle(bad)
        response=io.BytesIO(raw+b' ');response.status=200;opener.open.return_value=response
        with patch.object(prepare.urllib.request,'build_opener',return_value=opener),self.assertRaisesRegex(ValueError,'digest'):prepare.previous_bundle(env)
    def test_bundle_is_bounded_exact_and_approved_by_both_source_and_compiled_hashes(self):
        a=approved();a['source_hashes']={'a'*40:release.hashes(OLD),'b'*40:release.hashes({**OLD,**NEW})}
        proof={'contract':'harbor-static-bootstrap-v1','backend_revision':'a'*40,'revision':'b'*40,'before':a['before'],'after':a['after'],'source_hashes':a['source_hashes'],'cases':['old-html','new-html','old-delayed-script','new-delayed-script','rollback-old-html','rollback-live-new-page','cached-old-css','cached-new-page'],'passed':True}
        v={'version':1,'backend_revision':'a'*40,'revision':'b'*40,'before':a['before'],'after':a['after'],'source_hashes':a['source_hashes'],'sources':[],'assets':{k:base64.b64encode(x).decode() for k,x in NEW.items()},'compatibility':proof}
        a['compatibility_sha256']=release.digest(json.dumps(proof,sort_keys=True,separators=(',',':')).encode());raw=json.dumps(v).encode();a['bundle_sha256']=release.digest(raw)
        request={'action':'deploy-static','revision':'b'*40};self.assertEqual(release.bundle(io.BytesIO(raw),request,a),NEW)
        for bad in [{**request,'path':'/etc/shadow'},{**request,'revision':'d'*40},{'action':'deploy','revision':'b'*40}]:
            with self.assertRaises(ValueError):release.bundle(io.BytesIO(raw),bad,a)
        with self.assertRaises(ValueError):release.bundle(io.BytesIO(raw+b' '),request,a)
        with patch.object(release,'MAX_BUNDLE',len(raw)-1),self.assertRaises(ValueError):release.bundle(io.BytesIO(raw),request,a)
        v['assets']['../app.js']=v['assets'].pop('app.js');raw=json.dumps(v).encode();a['bundle_sha256']=release.digest(raw)
        with self.assertRaises(ValueError):release.bundle(io.BytesIO(raw),request,a)
    def test_real_file_commit_and_http_keep_legacy_assets_and_process_available(self):
        self.exercise(False)
    def test_failed_http_acceptance_rolls_back_only_html_and_preserves_late_new_pages(self):
        self.exercise(True)
    def exercise(self,fail):
        with tempfile.TemporaryDirectory() as directory:
            root=Path(directory);public=self.directory(root/'public',OLD);history=self.directory(root/'history',{});a=approved();owner=os.getuid();calls=[]
            class Handler(BaseHTTPRequestHandler):
                def log_message(self,*args):pass
                def do_GET(self):
                    if self.headers.get('Host')=='untrusted.invalid':code,body=403,b''
                    elif self.path=='/health':code,body=200,b'{}'
                    elif self.path=='/api/admin/state':code,body=401,b''
                    else:
                        name='index.html' if self.path=='/' else self.path[1:];code,body=200,(root/'public'/name).read_bytes()
                    self.send_response(code);self.send_header('Cache-Control','no-store');self.end_headers();self.wfile.write(body)
            server=ThreadingHTTPServer(('127.0.0.1',0),Handler);thread=threading.Thread(target=server.serve_forever,daemon=True);thread.start()
            def http(expected):
                release.check_http(server.server_port,expected);calls.append(expected)
                if fail and expected==a['after']:raise RuntimeError('Synthetic browser acceptance failure')
            try:
                verify=Mock()
                if fail:
                    with self.assertRaisesRegex(RuntimeError,'acceptance failure'):release.publish(public,NEW,a,verify,http,history,owner)
                else:receipt=release.publish(public,NEW,a,verify,http,history,owner);self.assertEqual(receipt['phase'],'active')
                self.assertEqual((root/'public'/'style.css').read_bytes(),OLD['style.css']);self.assertEqual((root/'public'/'app.js').read_bytes(),NEW['app.js'])
                self.assertEqual((root/'public'/'index.html').read_bytes(),OLD['index.html'] if fail else NEW['index.html'])
                self.assertEqual(json.loads((root/'history'/'backup.json').read_text())['app.js'],base64.b64encode(OLD['app.js']).decode())
                receipt=json.loads((root/'history'/'receipt.json').read_text());self.assertEqual(receipt['backend_image'],a['backend']['image_id']);self.assertEqual(receipt['phase'],'rolled_back' if fail else 'active');self.assertGreater(verify.call_count,4)
                self.assertTrue(thread.is_alive());self.assertTrue(server.fileno()>=0)
            finally:server.shutdown();server.server_close();thread.join(timeout=2);os.close(public);os.close(history)
    def test_unknown_return_after_index_replace_still_restores_index_by_actual_bytes(self):
        with tempfile.TemporaryDirectory() as directory:
            root=Path(directory);public=self.directory(root/'public',OLD);history=self.directory(root/'history',{});original=release.atomic_file;failed=False
            def uncertain(directory,name,content,*args,**kwargs):
                nonlocal failed
                original(directory,name,content,*args,**kwargs)
                if name=='index.html' and not failed:failed=True;raise OSError('Synthetic post-rename fsync uncertainty')
            try:
                with patch.object(release,'atomic_file',side_effect=uncertain),self.assertRaisesRegex(OSError,'uncertainty'):
                    release.publish(public,NEW,approved(),Mock(),Mock(),history,os.getuid())
                self.assertEqual((root/'public'/'index.html').read_bytes(),OLD['index.html']);self.assertEqual((root/'public'/'app.js').read_bytes(),NEW['app.js']);self.assertEqual(json.loads((root/'history'/'receipt.json').read_text())['phase'],'rolled_back')
            finally:os.close(public);os.close(history)
    def test_symlink_and_hardlink_assets_are_refused_and_drift_precedes_writes(self):
        with tempfile.TemporaryDirectory() as directory:
            root=Path(directory);public=self.directory(root/'public',OLD);history=self.directory(root/'history',{});owner=os.getuid()
            try:
                (root/'public'/'app.js').unlink();(root/'public'/'app.js').symlink_to(root/'public'/'style.css')
                with self.assertRaises(OSError):release.read_regular(public,'app.js',owner=owner)
                (root/'public'/'app.js').unlink();os.link(root/'public'/'style.css',root/'public'/'app.js')
                with self.assertRaises(ValueError):release.read_regular(public,'app.js',owner=owner)
                (root/'public'/'app.js').unlink();(root/'public'/'app.js').write_bytes(b'drift')
                with self.assertRaisesRegex(ValueError,'baseline'):release.publish(public,NEW,approved(),Mock(),Mock(),history,owner)
                self.assertEqual(list((root/'history').iterdir()),[])
                with self.assertRaises(ValueError):release.atomic_file(public,'../outside',b'no',owner=owner)
            finally:os.close(public);os.close(history)
    def test_process_identity_or_mount_changes_are_rejected_without_mutation(self):
        a=approved();a['backend'].update(container_id='1'*64,configured_image='gateway:'+'a'*40,pid=42,started_at='approved',start_ticks='123')
        row={'Id':'1'*64,'Image':a['backend']['image_id'],'Config':{'Image':a['backend']['configured_image'],'Labels':{'org.opencontainers.image.revision':'a'*40,'com.aistrong.project':'harbor-local-gateway'},'Cmd':['node','server.mjs'],'User':'node','Env':['HARBOR_PORT=8092']},'State':{'Running':True,'Health':{'Status':'healthy'},'Pid':42,'StartedAt':'approved'},'HostConfig':{'Privileged':False,'CapDrop':['ALL'],'PortBindings':{'8092/tcp':[{'HostPort':'8092'}]}},'Mounts':[]}
        api={'inspect_container':Mock(return_value=row),'NAME':'harbor-local-gateway','STACK':'/fixed','static_port':8092}
        with self.assertRaisesRegex(ValueError,'mounts'):release.Backend(api,a).verify()
        row['Id']='2'*64
        with self.assertRaisesRegex(ValueError,'immutable'):release.Backend(api,a).verify()
        self.assertEqual(set(api),{'inspect_container','NAME','STACK','static_port'})

if __name__=='__main__':unittest.main()
