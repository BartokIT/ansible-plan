'''
Tests for the command line front end.

cli.main is a client: it starts the backend if needed, POSTs the run
parameters and then only renders. These tests check the option parsing and
that client/backend contract, with the backend and the UI stubbed out.
'''
import os
import socket
import sys

import httpx
import pytest

from ansible_plan import cli, ipc


@pytest.fixture
def argv(monkeypatch):
    def _set(*args):
        monkeypatch.setattr(sys, 'argv', ['ansible-plan'] + list(args))
    return _set


# --------------------------------------------------------------------------
# option parsing
# --------------------------------------------------------------------------

def test_keyvalue_splits_on_the_first_equals():
    assert cli.keyvalue('key=value') == ['key', 'value']


def test_keyvalue_rejects_a_value_without_an_equals():
    with pytest.raises(Exception, match='malformatted'):
        cli.keyvalue('novalue')


def test_defaults_of_the_option_parser(argv):
    argv('workflow.yml')
    options = cli.read_options()

    assert options.workflow == 'workflow.yml'
    assert options.mode == 'stdout'
    assert options.log_level == 'info'
    assert options.log_dir == '/var/log/ansible/plan'
    assert options.verbosity == 0
    assert options.check_mode is False
    assert options.verify_only is False
    assert options.doubtful_mode is False
    assert options.interactive_retry is True
    assert options.input_templating == []


def test_execution_filters_are_parsed(argv):
    argv('workflow.yml', '-sn', 'n2', '-en', 'n5', '--skip-nodes', 'a,b')
    options = cli.read_options()

    assert options.start_from_node == 'n2'
    assert options.end_to_node == 'n5'
    assert options.skip_nodes == 'a,b'


def test_skip_and_execute_nodes_are_mutually_exclusive(argv):
    argv('workflow.yml', '--skip-nodes', 'a', '--execute-nodes', 'b')

    with pytest.raises(SystemExit):
        cli.read_options()


def test_input_templating_accumulates(argv):
    argv('workflow.yml', '-it', 'k1=v1', '--input-templating', 'k2=v2')

    assert cli.read_options().input_templating == [['k1', 'v1'], ['k2', 'v2']]


def test_the_socket_option_defaults_to_none(argv):
    argv('workflow.yml')

    assert cli.read_options().socket is None


def test_verbosity_counts_the_v_flags(argv):
    argv('workflow.yml', '-vvv')

    assert cli.read_options().verbosity == 3


@pytest.mark.parametrize('flags, attribute, expected', [
    (['--check'], 'check_mode', True),
    (['--verify-only'], 'verify_only', True),
    (['--doubtful-mode'], 'doubtful_mode', True),
    (['-nir'], 'interactive_retry', False),
    (['--log-dir-no-info'], 'log_dir_no_info', True),
    (['--draw'], 'draw_png', True),
])
def test_boolean_flags(argv, flags, attribute, expected):
    argv('workflow.yml', *flags)

    assert getattr(cli.read_options(), attribute) is expected


@pytest.mark.parametrize('option', ['--mode=nope', '--log-level=nope'])
def test_unknown_choices_are_rejected(argv, option):
    argv('workflow.yml', option)

    with pytest.raises(SystemExit):
        cli.read_options()


def test_extra_vars_come_from_the_ansible_option_helpers(argv):
    argv('workflow.yml', '--extra-vars', 'key=value')

    assert cli.read_options().extra_vars == ['key=value']


# --------------------------------------------------------------------------
# logging
# --------------------------------------------------------------------------

def test_define_logger_creates_the_main_log(tmp_path):
    logger = cli.define_logger(str(tmp_path / 'run'), 'debug')
    logger.info('hello')

    assert os.path.exists(str(tmp_path / 'run' / 'main.log'))


# --------------------------------------------------------------------------
# starting the backend
# --------------------------------------------------------------------------

class FakeResponse:
    def __init__(self, payload=None, status_code=200):
        self._payload = payload or {}
        self.status_code = status_code
        self.text = str(self._payload)

    def raise_for_status(self):
        return None

    def json(self):
        return self._payload


class StubClient:
    """
    Stands in for the httpx client bound to the session socket.

    ``failures`` makes the first N calls look like a backend that is not
    listening yet, which is how a socket with nothing behind it behaves.
    """

    def __init__(self, failures=0, always_fail=False, status='failed'):
        self.failures = failures
        self.always_fail = always_fail
        self.status = status
        self.calls = []
        self.posted = []
        self.post_error = None

    def get(self, path, **kwargs):
        self.calls.append(('GET', path))
        if self.always_fail or self.failures > 0:
            self.failures -= 1
            raise httpx.ConnectError('nothing is listening on the socket')
        return FakeResponse({'status': self.status})

    def post(self, path, json=None, timeout=None):
        self.calls.append(('POST', path))
        self.posted.append((path, json))
        if self.post_error:
            raise self.post_error
        return FakeResponse({'status': 'running'})


@pytest.fixture
def session_socket(tmp_path):
    return str(tmp_path / 's.sock')


def test_an_already_running_session_is_joined(monkeypatch, tmp_path, session_socket):
    # a session is shared: finding a backend means attaching to it, not
    # starting a second one
    spawned = []
    monkeypatch.setattr(cli.subprocess, 'Popen', lambda *a, **k: spawned.append(a))
    client = StubClient()

    result = cli.check_and_start_backend(cli.logging.getLogger('main'), str(tmp_path),
                                         session_socket, client)

    assert result is None
    assert spawned == []
    assert client.calls == [('GET', '/health')]


def test_a_missing_backend_is_spawned_detached(monkeypatch, tmp_path, session_socket):
    calls = {}

    def fake_popen(command, **kwargs):
        calls['command'] = command
        calls['kwargs'] = kwargs
        return 'the-process'

    monkeypatch.setattr(cli.subprocess, 'Popen', fake_popen)

    cli.check_and_start_backend(cli.logging.getLogger('main'), str(tmp_path),
                               session_socket, StubClient(failures=1))

    assert calls['command'] == [sys.executable, '-m', 'ansible_plan.service',
                                '--log-dir', str(tmp_path), '--socket', session_socket]
    # detached, so the session outlives the front end and can be re-joined
    assert calls['kwargs']['start_new_session'] is True


def test_a_socket_left_by_a_dead_backend_is_removed(monkeypatch, tmp_path, session_socket):
    # binding over an existing socket file fails with EADDRINUSE, so the
    # leftover of a crashed backend has to go before spawning a new one
    dead = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    dead.bind(session_socket)
    dead.close()
    assert os.path.exists(session_socket)

    monkeypatch.setattr(cli.subprocess, 'Popen', lambda *a, **k: 'the-process')

    cli.check_and_start_backend(cli.logging.getLogger('main'), str(tmp_path),
                               session_socket, StubClient(failures=1))

    assert not os.path.exists(session_socket)


def test_the_socket_directory_is_created_when_missing(monkeypatch, tmp_path):
    session_socket = str(tmp_path / 'run' / 's.sock')
    monkeypatch.setattr(cli.subprocess, 'Popen', lambda *a, **k: 'the-process')

    cli.check_and_start_backend(cli.logging.getLogger('main'), str(tmp_path),
                               session_socket, StubClient(failures=1))

    assert os.path.isdir(str(tmp_path / 'run'))


def test_a_backend_that_never_answers_aborts(monkeypatch, tmp_path, session_socket):
    monkeypatch.setattr(cli.subprocess, 'Popen', lambda *a, **k: 'the-process')
    monkeypatch.setattr(cli.time, 'sleep', lambda seconds: None)

    with pytest.raises(SystemExit):
        cli.check_and_start_backend(cli.logging.getLogger('main'), str(tmp_path),
                                    session_socket, StubClient(always_fail=True))


# --------------------------------------------------------------------------
# main: the request the CLI sends to the backend
# --------------------------------------------------------------------------

class StubConsole:
    answer = 'n'

    def __init__(self, *args, **kwargs):
        self.printed = []

    def print(self, *args, **kwargs):
        self.printed.append(args[0] if args else '')

    def input(self, prompt):
        return self.answer


class StubOutput:
    instances = []

    def __init__(self, **kwargs):
        self.kwargs = kwargs
        StubOutput.instances.append(self)

    def start(self):
        pass

    def join(self):
        pass

    def run(self):
        pass


@pytest.fixture
def stub_frontend(monkeypatch):
    StubOutput.instances = []
    client = StubClient()

    monkeypatch.setattr(cli, 'check_and_start_backend',
                        lambda logger, log_dir, socket_path, client: None)
    monkeypatch.setattr(cli.ipc, 'build_client', lambda path, **kwargs: client)
    monkeypatch.setattr(cli, 'Console', StubConsole)
    monkeypatch.setattr(cli, 'StdoutWorkflowOutput', StubOutput)
    monkeypatch.setattr(cli, 'TextualWorkflowOutput', StubOutput)
    return client


def test_main_sends_the_run_parameters_to_the_backend(argv, stub_frontend, tmp_path):
    argv('workflow.yml', '--log-dir', str(tmp_path), '--log-dir-no-info',
         '--check', '-sn', 'n2', '--skip-nodes', 'a,b',
         '-it', 'key=value', '--extra-vars', 'evar=1', '-vv')

    cli.main()

    url, payload = stub_frontend.posted[0]
    assert url == '/workflow'
    assert payload['workflow_file'] == os.path.abspath('workflow.yml')
    assert payload['check_mode'] is True
    assert payload['start_from_node'] == 'n2'
    assert payload['skip_nodes'] == ['a', 'b']
    assert payload['filter_nodes'] == []
    assert payload['input_templating'] == {'key': 'value'}
    assert payload['extra_vars'] == {'evar': '1'}
    assert payload['verbosity'] == 2
    assert payload['log_dir'] == str(tmp_path)


def test_main_names_the_log_directory_after_the_workflow_and_time(argv, stub_frontend, tmp_path):
    argv('examples/basic.yml', '--log-dir', str(tmp_path))

    cli.main()

    log_dir = stub_frontend.posted[0][1]['log_dir']
    assert log_dir.startswith(str(tmp_path) + '/basic.yml_')
    assert len(log_dir.rsplit('_', 2)[-1]) == 6   # HHMMSS


def test_main_uses_the_textual_frontend_in_visual_mode(argv, stub_frontend, tmp_path):
    argv('workflow.yml', '--log-dir', str(tmp_path), '--log-dir-no-info', '--mode', 'visual')

    cli.main()

    assert len(StubOutput.instances) == 1
    assert StubOutput.instances[0].kwargs['socket_path'] == ipc.DEFAULT_SOCKET_PATH


def test_the_session_socket_can_be_chosen_on_the_command_line(argv, stub_frontend, tmp_path):
    argv('workflow.yml', '--log-dir', str(tmp_path), '--log-dir-no-info',
         '--socket', str(tmp_path / 's.sock'))

    cli.main()

    assert StubOutput.instances[0].kwargs['socket_path'] == str(tmp_path / 's.sock')


def test_an_unusable_socket_path_stops_the_cli(argv, stub_frontend, tmp_path):
    argv('workflow.yml', '--log-dir', str(tmp_path), '--log-dir-no-info',
         '--socket', '/tmp/' + 'x' * 200 + '.sock')

    with pytest.raises(SystemExit) as exit_info:
        cli.main()

    assert exit_info.value.code == 1


def test_main_exits_when_the_user_declines_to_attach(argv, stub_frontend, tmp_path):
    conflict = httpx.HTTPStatusError(
        'conflict',
        request=httpx.Request('POST', ipc.BASE_URL + '/workflow'),
        response=httpx.Response(
            409,
            json={'detail': {'message': 'A different workflow is already running',
                             'running_workflow_file': '/tmp/other.yml'}},
            request=httpx.Request('POST', ipc.BASE_URL + '/workflow')),
    )

    stub_frontend.post_error = conflict
    StubConsole.answer = 'n'
    argv('workflow.yml', '--log-dir', str(tmp_path), '--log-dir-no-info')

    with pytest.raises(SystemExit) as exit_info:
        cli.main()

    assert exit_info.value.code == 0
