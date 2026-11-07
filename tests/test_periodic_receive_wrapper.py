import importlib.util
import os
from pathlib import Path
import unittest
from unittest.mock import patch

SOURCE = Path(__file__).resolve().parents[1] / 'ci/storage-recovery/periodic-receive.py'
spec = importlib.util.spec_from_file_location('periodic_receive', SOURCE)
wrapper = importlib.util.module_from_spec(spec)
spec.loader.exec_module(wrapper)


class PeriodicReceiveWrapperTest(unittest.TestCase):
    def test_ssh_allows_only_exact_protocol_command_without_arguments_or_terminal(self):
        self.assertEqual(wrapper.invocation([], 1000, 1000, 'harbor-periodic-receive', False), 'ssh')
        for command in [None, '', 'sh', 'harbor-periodic-receive extra', 'harbor-periodic-receive\n',
                        'harbor-periodic-receive; id', '/usr/bin/harbor-periodic-receive']:
            with self.subTest(command=command), self.assertRaises(wrapper.Refused):
                wrapper.invocation([], 1000, 1000, command, False)
        for args, uid, euid, tty in [(['--root-receive'], 1000, 1000, False),
                                    ([], 1000, 0, False), ([], 1000, 1000, True)]:
            with self.subTest(args=args, tty=tty), self.assertRaises(wrapper.Refused):
                wrapper.invocation(args, uid, euid, 'harbor-periodic-receive', tty)

    def test_root_does_not_accept_ssh_command_as_its_authority(self):
        self.assertEqual(wrapper.invocation(['--root-receive'], 0, 0, None, False), 'root')
        for args in [[], ['receive'], ['--root-receive', '/tmp/override']]:
            with self.subTest(args=args), self.assertRaises(wrapper.Refused):
                wrapper.invocation(args, 0, 0, 'harbor-periodic-receive', False)

    def test_unprivileged_handoff_has_exact_sudo_argv_and_clean_environment(self):
        with patch.object(wrapper, '__file__', str(wrapper.WRAPPER)), \
                patch.object(wrapper.sys, 'argv', [str(wrapper.WRAPPER)]), \
                patch.object(wrapper.os, 'getuid', return_value=1000), \
                patch.object(wrapper.os, 'geteuid', return_value=1000), \
                patch.object(wrapper.os, 'isatty', return_value=False), \
                patch.object(wrapper, 'trusted_file') as trust, \
                patch.object(wrapper.os, 'chdir'), patch.object(wrapper.os, 'umask'), \
                patch.object(wrapper.os, 'execve') as execute, \
                patch.dict(os.environ, {'SSH_ORIGINAL_COMMAND': 'harbor-periodic-receive',
                                        'NODE_OPTIONS': '--require=/tmp/foreign',
                                        'PYTHONPATH': '/tmp/foreign', 'LD_PRELOAD': '/tmp/foreign'}):
            wrapper.main()
        trust.assert_called_once_with(wrapper.WRAPPER)
        execute.assert_called_once_with('/usr/bin/sudo', ['/usr/bin/sudo', '-n', '--',
            '/usr/bin/python3', '-I', '-B', str(wrapper.WRAPPER), '--root-receive'],
            {'PATH': '/usr/bin:/bin', 'LANG': 'C'})

    def test_execution_dependency_tree_is_checked_before_fixed_node_exec(self):
        with patch.object(wrapper, 'trusted_file') as trust, \
                patch.object(wrapper.importlib.util, 'spec_from_file_location') as load, \
                patch.object(wrapper.importlib.util, 'module_from_spec') as module, \
                patch.object(wrapper.os, 'chdir'), patch.object(wrapper.os, 'execve') as execute:
            wrapper.receive()
            self.assertEqual([c.args for c in trust.call_args_list],
                             [(wrapper.NODE,), (wrapper.ENTRY,), (wrapper.INSTALLER,)])
            load.return_value.loader.exec_module.assert_called_once_with(module.return_value)
            module.return_value.verify_tool_tree.assert_called_once_with(wrapper.TOOLS)
            execute.assert_called_once_with(str(wrapper.NODE), [str(wrapper.NODE), str(wrapper.ENTRY),
                                                                  'receive'], wrapper.ENVIRONMENT)
            execute.reset_mock()
            module.return_value.verify_tool_tree.side_effect = ValueError('untrusted synthetic dependency')
            with self.assertRaises(ValueError):
                wrapper.receive()
            execute.assert_not_called()


if __name__ == '__main__':
    unittest.main()
