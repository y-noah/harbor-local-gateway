import {test} from 'node:test';
import assert from 'node:assert/strict';
import {networkConfig,acceptsRequest} from '../network.mjs';

test('default stays local and rejects forwarded host and cross-site origins',()=>{
  const config=networkConfig({});
  assert.equal(config.listenHost,'127.0.0.1');
  assert.equal(acceptsRequest({host:'localhost:43127'},config),true);
  assert.equal(acceptsRequest({host:'evil.test','x-forwarded-host':'localhost'},config),false);
  assert.equal(acceptsRequest({host:'localhost',origin:'https://evil.test'},config),false);
});
test('remote access requires explicit origin and exact host, scheme and port',()=>{
  assert.throws(()=>networkConfig({HARBOR_LISTEN_HOST:'0.0.0.0'}),/PUBLIC_ORIGINS/);
  const config=networkConfig({HARBOR_LISTEN_HOST:'0.0.0.0',HARBOR_PUBLIC_ORIGINS:'http://gateway.test:43127'});
  assert.equal(acceptsRequest({host:'gateway.test:43127',origin:'http://gateway.test:43127'},config),true);
  assert.equal(acceptsRequest({host:'gateway.test:43127'},config),true);
  for(const headers of [{host:'gateway.test'},{host:'gateway.test:43127',origin:'https://gateway.test:43127'},{host:'evil.test',origin:'http://gateway.test:43127'},{host:'gateway.test:43127',origin:'http://evil.test'}])assert.equal(acceptsRequest(headers,config),false);
});
test('invalid deployment configuration fails closed',()=>{
  for(const origin of ['*','http://gateway.test/path','http://user:pass@gateway.test','http://gateway.test/','ftp://gateway.test'])assert.throws(()=>networkConfig({HARBOR_PUBLIC_ORIGINS:origin}));
  for(const port of ['','0','65536','abc','1.5'])assert.throws(()=>networkConfig({HARBOR_PORT:port}));
  assert.equal(networkConfig({HARBOR_PORT:'43127'}).port,43127);
});
