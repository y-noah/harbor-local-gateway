#!/usr/bin/python3
"""从不可变 Git 版本生成同源兼容启动器；不修改 public 源文件。"""
import argparse
import base64
import hashlib
import json
from pathlib import Path
import re
import subprocess
import tempfile

FILES=('index.html','app.js','style.css')
REVISION=re.compile(r'^[a-f0-9]{40}$')
MAX_SOURCE=4*1024*1024
MAX_BRANCHES=16

def digest(data):return hashlib.sha256(data).hexdigest()
def encode(data):return base64.b64encode(data).decode()
def decode(data):return base64.b64decode(data,validate=True)
def hashes(files):return {name:digest(files[name]) for name in FILES}
def git_files(revision):
    if not REVISION.fullmatch(revision):raise ValueError('Expected immutable Git revision')
    result={name:subprocess.run(['git','show',revision+':public/'+name],capture_output=True,check=True).stdout for name in FILES}
    if any(len(data)>MAX_SOURCE for data in result.values()):raise ValueError('Static source exceeds limit')
    return result

def html_for(source,revision,baseline):
    html=source.decode('utf8')
    if 'data-harbor-static-revision' in html:raise ValueError('Original HTML already contains a release marker')
    if len(re.findall(r'<html(?:\s|>)',html,re.I))!=1:raise ValueError('Expected one HTML root')
    return source if revision==baseline else re.sub(r'<html(?=\s|>)','<html data-harbor-static-revision="'+revision+'"',html,count=1,flags=re.I).encode()

def compile_app(sources,baseline):
    parts=['// Harbor static bootstrap v1. Backend image/process are unchanged.\nconst __harborStaticRevision=document.documentElement.getAttribute("data-harbor-static-revision")||'+json.dumps(baseline)+';\n']
    for index,row in enumerate(sources):
        files={name:decode(data) for name,data in row['files'].items()};js=files['app.js'].decode('utf8');css=files['style.css'].decode('utf8')
        # These module features cannot be moved into a lexical branch unchanged.
        if re.search(r'\bimport\s*\.\s*meta\b',js):raise ValueError('Static module metadata requires separate qualification')
        if re.search(r'@import\b|url\s*\(',css,re.I):raise ValueError('Constructed stylesheet external resources require separate qualification')
        parts.append(('if' if index==0 else 'else if')+'(__harborStaticRevision==='+json.dumps(row['revision'])+'){\n')
        parts.append('{ const sheet=new CSSStyleSheet(); sheet.replaceSync('+json.dumps(css,ensure_ascii=True)+'); document.adoptedStyleSheets=[...document.adoptedStyleSheets,sheet]; for(const link of document.querySelectorAll(\'link[rel="stylesheet"]\')){if(new URL(link.href,location.href).pathname==="/style.css")link.remove();} }\n{\n'+js+'\n}\n}\n')
    parts.append('else { throw new Error("Harbor static revision is unavailable; reload the page"); }\n')
    result=''.join(parts).encode()
    if len(result)>MAX_SOURCE:raise ValueError('Compiled bootstrap exceeds limit; retain prior assets and use a full release route')
    with tempfile.NamedTemporaryFile(suffix='.mjs') as f:
        f.write(result);f.flush()
        try:subprocess.run(['node','--check',f.name],check=True,capture_output=True)
        except subprocess.CalledProcessError as error:raise ValueError('Static module cannot be wrapped without changing its syntax') from error
    return result

def build(revision,backend_revision,previous=None,previous_index_revision=None):
    baseline=git_files(backend_revision);candidate=git_files(revision)
    sources=[{'revision':backend_revision,'files':{k:encode(v) for k,v in baseline.items()}}]
    before=baseline
    if previous is not None:
        if previous.get('version')!=1 or previous.get('backend_revision')!=backend_revision:raise ValueError('Previous static bundle backend differs')
        sources=previous['sources']
        if not sources or sources[0]['revision']!=backend_revision or sources[0]['files']!={k:encode(v) for k,v in baseline.items()}:raise ValueError('Previous original branch differs')
        before={'index.html':decode(previous['assets']['index.html']),'app.js':decode(previous['assets']['app.js']),'style.css':baseline['style.css']}
        if previous_index_revision is not None:
            selected=next((row for row in sources if row['revision']==previous_index_revision),None)
            if selected is None:raise ValueError('Previous index revision is not retained')
            before['index.html']=html_for(decode(selected['files']['index.html']),previous_index_revision,backend_revision)
    if any(row['revision']==revision for row in sources):raise ValueError('Static revision is already retained')
    if len(sources)>=MAX_BRANCHES:raise ValueError('Static branch retention limit reached; use full release route')
    sources=[*sources,{'revision':revision,'files':{k:encode(v) for k,v in candidate.items()}}]
    app=compile_app(sources,backend_revision);index=html_for(candidate['index.html'],revision,backend_revision)
    after={'index.html':index,'app.js':app,'style.css':baseline['style.css']}
    return {'version':1,'revision':revision,'backend_revision':backend_revision,'sources':sources,'source_hashes':{row['revision']:hashes({k:decode(v) for k,v in row['files'].items()}) for row in sources},'before':hashes(before),'after':hashes(after),'assets':{'app.js':encode(app),'index.html':encode(index)},'compatibility':None}

if __name__=='__main__':
    parser=argparse.ArgumentParser();parser.add_argument('--revision',required=True);parser.add_argument('--backend-revision',required=True);parser.add_argument('--previous');parser.add_argument('--previous-index-revision');parser.add_argument('--output',required=True);a=parser.parse_args()
    bundle=build(a.revision,a.backend_revision,json.loads(Path(a.previous).read_text()) if a.previous else None,previous_index_revision=a.previous_index_revision)
    Path(a.output).write_text(json.dumps(bundle,sort_keys=True,separators=(',',':')))
    print(json.dumps({'revision':a.revision,'backend_revision':a.backend_revision,'retained_branches':len(bundle['sources']),'before':bundle['before'],'after':bundle['after'],'browser_verified':False}))
