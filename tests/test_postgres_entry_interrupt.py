import copy,importlib.util,json,os,subprocess,tempfile,time,unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import Mock,patch
ROOT=Path(__file__).parents[1]
def load(name,path):
 s=importlib.util.spec_from_file_location(name,ROOT/path);m=importlib.util.module_from_spec(s);s.loader.exec_module(m);return m
u=load('interrupt_upgrade','ci/postgres-upgrade-deploy.py')
fixtures=load('interrupt_fixtures','tests/test_postgres_runtime_upgrade.py')

class InterruptEntryTests(unittest.TestCase):
 def fixture(self):
  directory=tempfile.TemporaryDirectory();self.addCleanup(directory.cleanup);path=Path(directory.name);(path/'fd').mkdir();(path/'fd/a').symlink_to('socket:[11]');(path/'fd/b').symlink_to('socket:[12]');(path/'ns').touch()
  real_path=Path
  def paths(*parts):
   value='/'.join(str(p) for p in parts)
   return path/'fd' if value=='/proc/4321/fd' else path/'ns' if value in ['/proc/4321/ns/net','/proc/self/ns/net'] else real_path(*parts)
  row={'Id':'a'*64,'State':{'Pid':4321,'Running':True}}
  runtime=SimpleNamespace(inspect_entry=Mock(side_effect=lambda _:copy.deepcopy(row)))
  entry=Mock();entry.PROFILE=Path('/profile');entry.ENTRY_CONFIG=Path('/config');entry.ACTIVE=Path('/active');entry.STATE=Path('/state');entry.CONTROL='/control';entry.validate.side_effect=lambda v:v;entry.private_json.side_effect=lambda p:{entry.PROFILE:{'entryPort':18092,'entryCgroup':'/owned'},entry.ENTRY_CONFIG:{'listenPort':18092},entry.ACTIVE:{'candidate':{'containerId':'source'}},entry.STATE:{'phase':'activated','candidate':{'containerId':'source'},'activeRuntimeConfig':{'bridge':'owned'}}}[p];entry.process_ticks.return_value='100';entry.process_cgroup.return_value='/owned';entry.socket_owner.return_value=4321;entry.entry_state.return_value={'slot':'candidate','phase':'holding','epoch':7}
  api={'NAME':'harbor','inspect_container':Mock(return_value={'Id':'source'})};ticket={'interruptEntryClients':True};commands=[]
  def command(args,**kwargs):
   commands.append(args)
   if '-K' in args:return SimpleNamespace(returncode=0,stderr='',stdout='closed socket\n')
   return SimpleNamespace(returncode=0,stderr='',stdout='private-endpoints ino:'+('11' if 'listening' in args else '12')+' users:(("node",pid=4321,fd=5))\n')
  patches=[patch.object(u,'Path',side_effect=paths),patch.object(u,'load',return_value=runtime),patch.object(u,'trusted_code_bytes',return_value=b'trusted'),patch.object(u,'verify_source'),patch.object(u,'network_snapshot'),patch.object(subprocess,'run',side_effect=command)]
  for p in patches:p.start();self.addCleanup(p.stop)
  return entry,api,ticket,runtime,row,commands
 def run_interrupt(self,entry,api,ticket):return u.interrupt_entry_clients(entry,ticket,api,{'candidate':{'containerId':'source'}},time.monotonic()+30,7)
 def test_only_actual_entry_port_established_tcp_destroyed(self):
  entry,api,ticket,_,_,commands=self.fixture();result=self.run_interrupt(entry,api,ticket)
  self.assertEqual(result,{'tcpResetRequested':True,'observedClients':1,'reportedSockets':1});kill=next(c for c in commands if '-K' in c);self.assertIn('( sport = :18092 )',kill);self.assertNotIn('( sport = :8092 )',kill);self.assertIn('established',kill);self.assertNotIn('private-endpoints',json.dumps(result));self.assertEqual(kill[:5],['/usr/bin/nsenter','-t','4321','-n','/usr/bin/ss'])
 def test_wrong_control_pid_changed_container_and_epoch_refuse_before_kill(self):
  for change in ['pid','container','epoch']:
   with self.subTest(change=change):
    entry,api,ticket,runtime,row,commands=self.fixture()
    if change=='pid':entry.socket_owner.return_value=1234
    if change=='container':runtime.inspect_entry.side_effect=[row,dict(row,Id='b'*64)]
    if change=='epoch':entry.entry_state.return_value={'slot':'candidate','phase':'holding','epoch':8}
    with self.assertRaises(RuntimeError):self.run_interrupt(entry,api,ticket)
    self.assertFalse(any('-K' in c for c in commands))
 def test_other_or_unknown_socket_owner_refuses(self):
  entry,api,ticket,_,_,commands=self.fixture()
  def foreign(args,**kw):commands.append(args);return SimpleNamespace(returncode=0,stderr='',stdout='endpoints ino:99 users:(("other",pid=999,fd=5))\n')
  with patch.object(subprocess,'run',side_effect=foreign),self.assertRaisesRegex(RuntimeError,'socket ownership unknown'):self.run_interrupt(entry,api,ticket)
  self.assertFalse(any('-K' in c for c in commands))
 def test_ss_stderr_even_exit_zero_is_not_success(self):
  entry,api,ticket,_,_,commands=self.fixture();original=subprocess.run
  def unsupported(args,**kw):
   if '-K' in args:return SimpleNamespace(returncode=0,stderr='SOCK_DESTROY answers: Operation not supported',stdout='private endpoints')
   return original(args,**kw)
  with patch.object(subprocess,'run',side_effect=unsupported),self.assertRaisesRegex(RuntimeError,'destroy unsupported or unconfirmed'):self.run_interrupt(entry,api,ticket)
 def test_8092_or_mismatching_port_refuses(self):
  entry,api,ticket,_,_,commands=self.fixture();original=entry.private_json.side_effect
  entry.private_json.side_effect=lambda p:{'listenPort':8092} if p==entry.ENTRY_CONFIG else original(p)
  with self.assertRaisesRegex(RuntimeError,'listener configuration'):self.run_interrupt(entry,api,ticket)
  self.assertEqual(commands,[])
 def test_no_connections_does_not_claim_reset(self):
  entry,api,ticket,_,_,commands=self.fixture();original=subprocess.run
  def empty(args,**kw):return SimpleNamespace(returncode=0,stderr='',stdout='') if 'established' in args else original(args,**kw)
  with patch.object(subprocess,'run',side_effect=empty):self.assertEqual(self.run_interrupt(entry,api,ticket),{'tcpResetRequested':False,'observedClients':0})
  self.assertFalse(any('-K' in c for c in commands))
 def test_ticket_flag_is_strict_boolean_default_off(self):
  for value in [1,'true',None,{},[]]:
   with self.assertRaisesRegex(RuntimeError,'must be boolean'):u.validate_ticket({**fixtures.ticket(),'interruptEntryClients':value},fixtures.IMAGE,fixtures.CONFIG)
  for extra in [{},{'interruptEntryClients':False},{'interruptEntryClients':True}]:u.validate_ticket({**fixtures.ticket(),**extra},fixtures.IMAGE,fixtures.CONFIG)

if __name__=='__main__':unittest.main()
