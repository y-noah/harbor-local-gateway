#!/usr/bin/python3
"""CI 准备入口：仅从同项目指定作业读取摘要批准的上次公开静态产物。"""
import hashlib
import importlib.util
import json
import os
from pathlib import Path
import re
import urllib.parse
import urllib.request

spec=importlib.util.spec_from_file_location('static_builder',Path(__file__).with_name('static-release-build.py'))
builder=importlib.util.module_from_spec(spec);spec.loader.exec_module(builder)
MAX_PREVIOUS=12*1024*1024

def previous_bundle(env):
    job=env.get('STATIC_PREVIOUS_JOB_ID','');sha=env.get('STATIC_PREVIOUS_BUNDLE_SHA256','')
    if not job and not sha:return None
    if not re.fullmatch('[1-9][0-9]{0,15}',job) or not re.fullmatch('[a-f0-9]{64}',sha):raise ValueError('Previous static job and digest must be supplied together')
    project=env.get('CI_PROJECT_ID','');api=urllib.parse.urlsplit(env.get('CI_API_V4_URL',''));origin=urllib.parse.urlsplit(env.get('CI_PROJECT_URL',''))
    if not re.fullmatch('[1-9][0-9]{0,15}',project) or api.scheme not in ('http','https') or (api.scheme,api.netloc)!=(origin.scheme,origin.netloc) or api.path!='/api/v4' or api.username or api.password or api.query or api.fragment:raise ValueError('Previous artifact must use this GitLab project origin')
    token=env.get('CI_JOB_TOKEN','')
    if not token:raise ValueError('CI job artifact token is required')
    url=urllib.parse.urlunsplit((api.scheme,api.netloc,api.path+'/projects/'+project+'/jobs/'+job+'/artifacts/.ci-static/release.json','',''))
    class NoRedirect(urllib.request.HTTPRedirectHandler):
        def redirect_request(self,*args):return None
    opener=urllib.request.build_opener(urllib.request.ProxyHandler({}),NoRedirect())
    with opener.open(urllib.request.Request(url,headers={'JOB-TOKEN':token}),timeout=30) as response:
        if response.status!=200:raise ValueError('Previous static artifact is unavailable')
        raw=response.read(MAX_PREVIOUS+1)
    if len(raw)>MAX_PREVIOUS or hashlib.sha256(raw).hexdigest()!=sha:raise ValueError('Previous static artifact digest differs')
    value=json.loads(raw)
    if value.get('compatibility',{}).get('passed') is not True:raise ValueError('Previous static artifact lacks qualification')
    return value

def prepare(env):
    previous=previous_bundle(env)
    if env.get('STATIC_PREVIOUS_INDEX_REVISION') and previous is None:raise ValueError('Previous index selection requires a qualified prior bundle')
    value=builder.build(env['CI_COMMIT_SHA'],env['STATIC_BACKEND_REVISION'],previous,previous_index_revision=env.get('STATIC_PREVIOUS_INDEX_REVISION') or None)
    directory=Path('.ci-static');directory.mkdir(exist_ok=True)
    (directory/'release.json').write_text(json.dumps(value,sort_keys=True,separators=(',',':')))
    print(json.dumps({'revision':value['revision'],'backend_revision':value['backend_revision'],'retained_branches':len(value['sources']),'previous_artifact_bound':previous is not None,'browser_verified':False}))

if __name__=='__main__':prepare(os.environ)
