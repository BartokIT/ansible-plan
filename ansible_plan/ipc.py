'''
Local IPC between the front ends and the backend service.

The backend listens on a unix domain socket rather than a TCP port, so a
session is reachable only from the machine it runs on: a port forward cannot
expose it, and access is decided by the filesystem.

A session is deliberately shared. Every member of the group that owns the
socket directory attaches to the same live workflow and controls it - starts,
stops, retries, answers checkpoints. The access gate is the **directory**, not
the socket file: a process that cannot traverse the directory cannot reach the
socket whatever its mode is. The directory is created rwxrws--- so that any
member of the group can create and replace the session socket, and the setgid
bit makes the socket inherit that group whoever starts the backend.
'''
import os
import shutil
import socket
import stat

import httpx

DEFAULT_SOCKET_PATH = '/run/ansible-plan/ansible-plan.sock'

#: Overrides the socket location, for setups where /run is not writable.
SOCKET_ENV_VAR = 'ANSIBLE_PLAN_SOCKET'
#: Group that owns a socket directory created by ansible-plan itself.
GROUP_ENV_VAR = 'ANSIBLE_PLAN_SOCKET_GROUP'

SOCKET_DIR_MODE = 0o2770
SOCKET_MODE = 0o660

# sockaddr_un.sun_path holds 108 bytes including the terminator
MAX_SOCKET_PATH = 107

# httpx wants a URL even when the connection goes through a socket; the host
# part is never resolved.
BASE_URL = 'http://ansible-plan'


class SocketError(Exception):
    ''' The session socket cannot be used '''


def socket_path(override: str = None) -> str:
    '''
    Resolve the socket to talk to: an explicit path wins over the environment,
    which wins over the default location.
    '''
    path = override or os.environ.get(SOCKET_ENV_VAR) or DEFAULT_SOCKET_PATH
    path = os.path.abspath(path)
    if len(path.encode('utf-8')) > MAX_SOCKET_PATH:
        raise SocketError(
            'The socket path is %s bytes long, the limit for a unix socket is %s: %s'
            % (len(path.encode('utf-8')), MAX_SOCKET_PATH, path))
    return path


def ensure_socket_dir(path: str, group: str = None):
    '''
    Make sure the directory holding the socket exists and is reachable by the
    group sharing the session.

    An existing directory is left untouched: its permissions are how an
    administrator decided who may join a session.

    Args:
        path (string): The socket path, not the directory.
        group (string): Group to own a directory created here, by name or gid.
    Raises:
        SocketError: If the directory cannot be created or handed to the group.
    '''
    directory = os.path.dirname(path)
    if os.path.isdir(directory):
        return

    try:
        os.makedirs(directory)
        # makedirs applies the umask, and the setgid bit needs a chmod anyway
        os.chmod(directory, SOCKET_DIR_MODE)
    except OSError as err:
        raise SocketError(
            'Cannot create the socket directory %s: %s. Create it yourself or '
            'point %s somewhere writable.' % (directory, err, SOCKET_ENV_VAR))

    if group:
        try:
            shutil.chown(directory, group=group)
        except (LookupError, OSError, PermissionError) as err:
            raise SocketError('Cannot give the socket directory %s to the group %s: %s'
                              % (directory, group, err))


def is_listening(path: str) -> bool:
    '''Tell a live session socket from one left behind by a dead backend.'''
    probe = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    probe.settimeout(0.5)
    try:
        probe.connect(path)
        return True
    except OSError:
        return False
    finally:
        probe.close()


def remove_stale_socket(path: str) -> bool:
    '''
    Remove the socket a crashed backend left behind, which would otherwise
    make the next bind fail with EADDRINUSE.

    A socket with a backend still listening is never removed, so starting a
    second front end cannot cut off a running session.

    Returns:
        bool: True if a stale socket was removed.
    Raises:
        SocketError: If the path holds something that is not a socket, or the
            caller is not allowed to reach it.
    '''
    try:
        mode = os.stat(path).st_mode
    except FileNotFoundError:
        return False
    except PermissionError as err:
        # the caller is not in the group owning the socket directory, which is
        # exactly what the directory permissions are there to enforce
        raise SocketError(
            'Cannot reach the session at %s: %s. Joining a session needs '
            'membership of the group owning %s.'
            % (path, err, os.path.dirname(path)))

    if not stat.S_ISSOCK(mode):
        raise SocketError('%s exists and is not a socket, refusing to remove it' % path)

    if is_listening(path):
        return False

    try:
        os.unlink(path)
    except FileNotFoundError:      # another front end got there first
        return False
    except OSError as err:
        raise SocketError('Cannot remove the stale socket %s: %s' % (path, err))
    return True


def build_client(path: str, **kwargs) -> httpx.Client:
    '''An httpx client that reaches the backend through its unix socket.'''
    return httpx.Client(base_url=BASE_URL,
                        transport=httpx.HTTPTransport(uds=path),
                        **kwargs)
