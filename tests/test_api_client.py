'''
ApiClient must never raise when the backend cannot answer: the UI polls it
from worker threads, and an exception there takes the whole front end down.
The typical case is the CLI shutting the backend down while a poll is in
flight, which surfaces as a reset connection (httpx.ReadError).
'''
import httpx
import pytest

from ansible_plan.ui.api_client import ApiClient


def _client(handler):
    api = ApiClient('/nonexistent/ansible-plan.sock')
    api.client = httpx.Client(base_url='http://ansible-plan', transport=httpx.MockTransport(handler))
    return api


def _raise(exc_type):
    def handler(request):
        raise exc_type('[Errno 104] Connection reset by peer', request=request)
    return handler


QUERIES = [
    ('check_health', (), False),
    ('get_workflow_status', (), None),
    ('get_all_nodes', (), None),
    ('get_workflow_graph', (), None),
    ('get_node_stdout', ('n1',), None),
]

COMMANDS = [
    ('stop_workflow', ()),
    ('pause_workflow', ()),
    ('resume_workflow', ()),
    ('shutdown_backend', ()),
    ('restart_node', ('n1',)),
    ('skip_node', ('n1',)),
    ('approve_node', ('n1',)),
    ('disapprove_node', ('n1',)),
]

TRANSPORT_ERRORS = [httpx.ConnectError, httpx.ReadError, httpx.WriteError,
                    httpx.RemoteProtocolError, httpx.ReadTimeout]


@pytest.mark.parametrize('exc_type', TRANSPORT_ERRORS)
@pytest.mark.parametrize('method,args,expected', QUERIES)
def test_queries_swallow_transport_errors(method, args, expected, exc_type):
    assert getattr(_client(_raise(exc_type)), method)(*args) is expected


@pytest.mark.parametrize('exc_type', TRANSPORT_ERRORS)
@pytest.mark.parametrize('method,args', COMMANDS)
def test_commands_swallow_transport_errors(method, args, exc_type):
    assert getattr(_client(_raise(exc_type)), method)(*args) is None


@pytest.mark.parametrize('method,args,expected', QUERIES)
def test_queries_swallow_error_statuses(method, args, expected):
    assert getattr(_client(lambda request: httpx.Response(500)), method)(*args) is expected


def test_check_health_true_when_backend_answers():
    assert _client(lambda request: httpx.Response(200, json={'status': 'ok'})).check_health() is True
