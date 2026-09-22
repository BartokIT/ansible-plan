'''
Tests for the local IPC layer.

A session lives on a unix socket: reachable only from the machine, shared by
every member of the group that owns the socket directory. These tests cover
how that path is resolved and guarded, and end with the real thing - a backend
started as a subprocess and reached over its socket.
'''
import grp
import os
import shutil
import socket
import stat
import subprocess
import sys
import tempfile
import time

import httpx
import pytest

from ansible_plan import ipc

REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


@pytest.fixture
def short_socket():
    '''
    A socket path well inside the 107 byte limit.

    pytest's own tmp_path is close enough to the limit that a nested socket
    name can push a test over it.
    '''
    directory = tempfile.mkdtemp(prefix='ap')
    yield os.path.join(directory, 's.sock')
    shutil.rmtree(directory, ignore_errors=True)


@pytest.fixture
def own_group():
    return grp.getgrgid(os.getgid()).gr_name


# --------------------------------------------------------------------------
# resolving the socket path
# --------------------------------------------------------------------------

def test_the_default_socket_is_shared_not_per_user():
    # a per user path would defeat the point: the second user has to land on
    # the same socket to join the session
    assert ipc.socket_path() == ipc.DEFAULT_SOCKET_PATH
    assert '%' not in ipc.DEFAULT_SOCKET_PATH


def test_an_explicit_path_wins_over_the_environment(monkeypatch):
    monkeypatch.setenv(ipc.SOCKET_ENV_VAR, '/tmp/from-env.sock')

    assert ipc.socket_path('/tmp/explicit.sock') == '/tmp/explicit.sock'


def test_the_environment_wins_over_the_default(monkeypatch):
    monkeypatch.setenv(ipc.SOCKET_ENV_VAR, '/tmp/from-env.sock')

    assert ipc.socket_path() == '/tmp/from-env.sock'


def test_a_relative_path_is_made_absolute(monkeypatch, tmp_path):
    monkeypatch.chdir(tmp_path)

    assert ipc.socket_path('s.sock') == str(tmp_path / 's.sock')


def test_a_path_too_long_for_a_unix_socket_is_refused():
    with pytest.raises(ipc.SocketError, match='limit for a unix socket'):
        ipc.socket_path('/tmp/' + 'x' * 200 + '.sock')


# --------------------------------------------------------------------------
# the directory is the access gate
# --------------------------------------------------------------------------

def test_a_created_directory_is_shared_with_the_group(tmp_path):
    path = str(tmp_path / 'run' / 's.sock')

    ipc.ensure_socket_dir(path)

    mode = stat.S_IMODE(os.stat(str(tmp_path / 'run')).st_mode)
    # rwxrws---: the group can create and replace the socket, nobody else can
    # even traverse; setgid so the socket inherits the group
    assert mode == ipc.SOCKET_DIR_MODE
    assert mode & stat.S_ISGID
    assert not mode & stat.S_IRWXO


def test_an_existing_directory_is_left_as_the_administrator_set_it(tmp_path):
    directory = tmp_path / 'run'
    directory.mkdir()
    os.chmod(str(directory), 0o700)

    ipc.ensure_socket_dir(str(directory / 's.sock'))

    assert stat.S_IMODE(os.stat(str(directory)).st_mode) == 0o700


def test_the_directory_can_be_handed_to_a_named_group(tmp_path, own_group):
    path = str(tmp_path / 'run' / 's.sock')

    ipc.ensure_socket_dir(path, group=own_group)

    gid = os.stat(str(tmp_path / 'run')).st_gid
    assert grp.getgrgid(gid).gr_name == own_group


def test_an_unknown_group_is_reported(tmp_path):
    path = str(tmp_path / 'run' / 's.sock')

    with pytest.raises(ipc.SocketError, match='no-such-group'):
        ipc.ensure_socket_dir(path, group='no-such-group')


def test_a_directory_that_cannot_be_created_is_reported(tmp_path):
    blocker = tmp_path / 'blocker'
    blocker.write_text('not a directory')

    with pytest.raises(ipc.SocketError, match='Cannot create the socket directory'):
        ipc.ensure_socket_dir(str(blocker / 'run' / 's.sock'))


# --------------------------------------------------------------------------
# sockets left behind by a dead backend
# --------------------------------------------------------------------------

def bind_socket(path):
    sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    sock.bind(path)
    return sock


def test_nothing_to_remove_when_there_is_no_socket(short_socket):
    assert ipc.remove_stale_socket(short_socket) is False


def test_a_socket_with_nobody_behind_it_is_removed(short_socket):
    bind_socket(short_socket).close()

    assert ipc.remove_stale_socket(short_socket) is True
    assert not os.path.exists(short_socket)


def test_a_live_session_socket_is_never_removed(short_socket):
    listener = bind_socket(short_socket)
    listener.listen(1)
    try:
        assert ipc.is_listening(short_socket) is True
        # joining a session must not be able to cut off the running one
        assert ipc.remove_stale_socket(short_socket) is False
        assert os.path.exists(short_socket)
    finally:
        listener.close()


def test_a_caller_outside_the_group_gets_a_clear_message(short_socket, monkeypatch):
    # the directory permissions refuse the stat; the front end must say why
    # instead of dying on a bare PermissionError
    bind_socket(short_socket).close()

    def refuse(path):
        raise PermissionError(13, 'Permission denied')

    monkeypatch.setattr(ipc.os, 'stat', refuse)

    with pytest.raises(ipc.SocketError, match='membership of the group'):
        ipc.remove_stale_socket(short_socket)


def test_a_regular_file_in_the_way_is_not_deleted(short_socket):
    with open(short_socket, 'w') as handle:
        handle.write('not a socket')

    with pytest.raises(ipc.SocketError, match='not a socket'):
        ipc.remove_stale_socket(short_socket)
    assert os.path.exists(short_socket)


# --------------------------------------------------------------------------
# the real thing
# --------------------------------------------------------------------------

def test_the_backend_is_reachable_over_its_socket_and_only_by_the_group(short_socket, tmp_path):
    log_dir = str(tmp_path / 'logs')
    os.makedirs(log_dir)
    process = subprocess.Popen(
        [sys.executable, '-m', 'ansible_plan.service',
         '--socket', short_socket, '--log-dir', log_dir, '--log-level', 'warning'],
        cwd=REPO_ROOT,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )

    try:
        client = ipc.build_client(short_socket)
        deadline = time.time() + 30
        response = None
        while time.time() < deadline:
            if process.poll() is not None:
                raise AssertionError('the backend exited: %s'
                                     % process.stderr.read().decode('utf-8', 'replace'))
            try:
                response = client.get('/health')
                break
            except httpx.ConnectError:
                time.sleep(0.2)

        assert response is not None, 'the backend never started listening'
        assert response.json() == {'status': 'ok'}

        # uvicorn opens a unix socket world writable; it must not stay that way
        assert stat.S_IMODE(os.stat(short_socket).st_mode) == ipc.SOCKET_MODE

        # a second front end joins the same session over the same socket
        assert ipc.build_client(short_socket).get('/workflow').status_code == 200
    finally:
        process.terminate()
        process.wait(timeout=15)


def test_a_client_cannot_reach_a_socket_it_cannot_traverse_to(short_socket):
    # the directory permissions, not the socket mode, are what keeps a
    # non member out; reproduce the refusal with the traverse bit removed
    directory = os.path.dirname(short_socket)
    listener = bind_socket(short_socket)
    listener.listen(1)
    os.chmod(directory, 0o000)
    try:
        if os.geteuid() == 0:
            pytest.skip('root bypasses directory permissions')
        with pytest.raises(httpx.ConnectError):
            ipc.build_client(short_socket).get('/health')
    finally:
        os.chmod(directory, 0o700)
        listener.close()
