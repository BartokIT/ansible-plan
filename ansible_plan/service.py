import argparse
import logging
import os
import json
import signal
import threading
import time
from fastapi import FastAPI, HTTPException, BackgroundTasks
from pydantic import BaseModel, Field
from typing import Optional, Dict, List

from . import ipc
from .core.loader import WorkflowYamlLoader
from .core.engine import AnsibleWorkflow
from .core.models import WorkflowStatus, PNode, INode, CNode
from .core.exceptions import (
    AnsibleWorkflowLoadingError,
    AnsibleWorkflowValidationError,
    AnsibleWorkflowVaultScript,
)
import jinja2


class StopWorkflowRequest(BaseModel):
    mode: str = "graceful"


app = FastAPI()

# Global state
workflow_lock = threading.Lock()
current_workflow: Optional[AnsibleWorkflow] = None


class WorkflowStartRequest(BaseModel):
    workflow_file: str
    extra_vars: Dict = Field(default_factory=dict)
    input_templating: Dict = Field(default_factory=dict)
    check_mode: bool = False
    verbosity: int = 0
    start_from_node: Optional[str] = None
    end_to_node: Optional[str] = None
    skip_nodes: List[str] = Field(default_factory=list)
    filter_nodes: List[str] = Field(default_factory=list)
    log_dir: str = "logs"
    log_level: str = "info"
    verify_only: bool = False
    doubtful_mode: bool = False


@app.post("/workflow")
async def start_workflow(request: WorkflowStartRequest, background_tasks: BackgroundTasks):
    global current_workflow
    with workflow_lock:
        if current_workflow and current_workflow.get_running_status() in [WorkflowStatus.RUNNING,
                                                                          WorkflowStatus.PAUSED,
                                                                          WorkflowStatus.ENDED,
                                                                          WorkflowStatus.FAILED]:
            if current_workflow.get_workflow_file() == request.workflow_file:
                return {"status": "reconnected"}
            else:
                raise HTTPException(status_code=409, detail={
                    "message": "A different workflow is already running",
                    "running_workflow_file": current_workflow.get_workflow_file()
                })

        try:
            loader = WorkflowYamlLoader(
                request.workflow_file,
                request.log_dir,
                request.log_level,
                request.input_templating,
                request.check_mode,
                request.verbosity,
                request.doubtful_mode,
            )
            aw = loader.parse(request.extra_vars)
            current_workflow = aw
        except (
            AnsibleWorkflowLoadingError,
            jinja2.exceptions.UndefinedError,
            AnsibleWorkflowVaultScript,
            AnsibleWorkflowValidationError,
        ) as e:
            aw = AnsibleWorkflow(
                workflow_file=request.workflow_file,
                logging_dir=request.log_dir,
                log_level=request.log_level,
                doubtful_mode=request.doubtful_mode,
            )
            aw.add_validation_error(str(e))
            aw.set_status(WorkflowStatus.FAILED)
            current_workflow = aw
            # Use a 422 status code for validation errors, as this is more specific than a generic 500.
            raise HTTPException(status_code=422, detail={"validation_errors": [str(e)]})

        if request.filter_nodes:
            aw.set_filtered_nodes(request.filter_nodes)
        if request.skip_nodes:
            aw.set_skipped_nodes(request.skip_nodes)

        start_node = request.start_from_node if request.start_from_node else '_s'
        end_node = request.end_to_node if request.end_to_node else '_e'

        background_tasks.add_task(aw.run, start_node=start_node, end_node=end_node, verify_only=request.verify_only)

    return {"status": WorkflowStatus.RUNNING}


@app.get("/workflow")
def get_workflow_status():
    with workflow_lock:
        if not current_workflow:
            return {"status": WorkflowStatus.NOT_STARTED}

        status = current_workflow.get_running_status()
        response = {"status": status}
        if status == WorkflowStatus.FAILED:
            errors = current_workflow.get_validation_errors()
            if errors:
                response["validation_errors"] = errors
        return response


@app.get("/workflow/nodes")
def get_workflow_nodes():
    with workflow_lock:
        if not current_workflow:
            return []

        nodes_data = []
        all_node_datas = current_workflow.get_node_datas()
        for node_id in current_workflow.get_nodes():
            node_obj = current_workflow.get_node_object(node_id)
            status = node_obj.get_status()
            node_info = {
                "id": node_obj.get_id(),
                "status": status.value if hasattr(status, 'value') else status,
                "type": node_obj.get_type(),
            }

            if node_info['type'] == 'block':
                node_data = all_node_datas.get(node_id, {})
                if 'child' in node_data and 'strategy' in node_data['child']:
                    node_info['strategy'] = node_data['child']['strategy']

            if isinstance(node_obj, PNode):
                node_info.update({
                    "playbook": node_obj.get_playbook(),
                    "inventory": node_obj.get_inventory(),
                    "extravars": node_obj.get_extravars(),
                    "description": node_obj.get_description(),
                    "reference": node_obj.get_reference(),
                })
                node_info.update(node_obj.get_telemetry())
            elif isinstance(node_obj, (INode, CNode)):
                node_info.update({
                    "description": node_obj.get_description(),
                    "reference": node_obj.get_reference(),
                })
            nodes_data.append(node_info)
        return nodes_data


@app.get("/workflow/graph")
def get_workflow_graph():
    with workflow_lock:
        if not current_workflow:
            raise HTTPException(status_code=404, detail="Workflow not found.")
        return {"edges": current_workflow.get_original_graph_edges()}


@app.get("/workflow/node/{node_id}/stdout")
def get_node_stdout(node_id: str):
    with workflow_lock:
        if not current_workflow:
            raise HTTPException(status_code=404, detail="Workflow not found.")

        logging_dir = current_workflow.get_logging_dir()
        node_obj = current_workflow.get_node_object(node_id)
        if not isinstance(node_obj, PNode):
            raise HTTPException(status_code=404, detail="Node is not a playbook node.")

        ident = getattr(node_obj, 'ident', node_id)
        stdout_path = os.path.join(logging_dir, ident, "stdout")

        if not os.path.exists(stdout_path):
            return {"stdout": ""}

        with open(stdout_path, "r") as f:
            return {"stdout": f.read()}


@app.post("/workflow/stop")
def stop_workflow(request: StopWorkflowRequest):
    with workflow_lock:
        if not current_workflow or current_workflow.get_running_status() not in [WorkflowStatus.RUNNING, WorkflowStatus.PAUSED]:
            raise HTTPException(status_code=404, detail="No running or paused workflow to stop.")
        current_workflow.stop(request.mode)
    return {"message": "Workflow stopping."}


@app.post("/workflow/pause")
def pause_workflow():
    with workflow_lock:
        if not current_workflow or current_workflow.get_running_status() != WorkflowStatus.RUNNING:
            raise HTTPException(status_code=404, detail="No running workflow to pause.")
        current_workflow.pause()
    return {"message": "Workflow paused."}


@app.post("/workflow/resume")
def resume_workflow():
    with workflow_lock:
        if not current_workflow or current_workflow.get_running_status() != WorkflowStatus.PAUSED:
            raise HTTPException(status_code=404, detail="No paused workflow to resume.")
        current_workflow.resume()
    return {"message": "Workflow resumed."}


@app.post("/workflow/node/{node_id}/restart")
def restart_node(node_id: str):
    with workflow_lock:
        if not current_workflow:
            raise HTTPException(status_code=404, detail="Workflow not found.")

        current_workflow.restart_failed_node(node_id)

    return {"message": f"Node {node_id} restarting."}


@app.post("/workflow/node/{node_id}/skip")
def skip_node(node_id: str):
    with workflow_lock:
        if not current_workflow:
            raise HTTPException(status_code=404, detail="Workflow not found.")

        current_workflow.skip_failed_node(node_id)

    return {"message": f"Node {node_id} skipped."}


@app.post("/workflow/node/{node_id}/approve")
def approve_node(node_id: str):
    with workflow_lock:
        if not current_workflow:
            raise HTTPException(status_code=404, detail="Workflow not found.")
        current_workflow.approve_node(node_id)
    return {"message": f"Node {node_id} approved."}


@app.post("/workflow/node/{node_id}/disapprove")
def disapprove_node(node_id: str):
    with workflow_lock:
        if not current_workflow:
            raise HTTPException(status_code=404, detail="Workflow not found.")
        current_workflow.disapprove_node(node_id)
    return {"message": f"Node {node_id} disapproved."}


@app.post("/shutdown")
def shutdown():
    with workflow_lock:
        if current_workflow:
            if current_workflow.get_running_status() == WorkflowStatus.RUNNING:
                raise HTTPException(status_code=409, detail="Cannot shutdown while a workflow is running.")
            # Tell the workflow thread to stop
            current_workflow.stop()
            # Give the thread a moment to stop
            time.sleep(1.5)

        # This is a simple way to shutdown for this app.
        # In a real production app, a more graceful shutdown mechanism would be needed.
        os.kill(os.getpid(), signal.SIGTERM)

    return {"message": "Shutting down."}


@app.get("/health")
def health_check():
    return {"status": "ok"}


def define_logger(logging_dir, level):
    common_format = "%(asctime)s [%(levelname)s] %(name)s: %(message)s"
    uvicorn_log_config_file = os.path.join(logging_dir, 'uvicorn_log_config.json')
    logger_file_path = os.path.join(logging_dir, 'service.log')
    if not os.path.exists(os.path.dirname(logger_file_path)):
        os.makedirs(os.path.dirname(logger_file_path))

    logger = logging.getLogger('main')
    if level:
        logger.setLevel(getattr(logging, level.upper()))

    logging.basicConfig(
        level=logging.DEBUG,
        format=common_format,
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    conf_uvicorn = dict(version=1)
    conf_uvicorn['handlers'] = dict(default={'formatter': 'default', 'class': 'logging.FileHandler', 'filename': logger_file_path})
    conf_uvicorn['loggers'] = {'uvicorn.access': {'level': 'ERROR'}}
    conf_uvicorn['formatters'] = {'default': {'format': common_format}}
    conf_uvicorn['root'] = {'level': level.upper(), 'handlers': ['default'], 'propgate': True}
    with open(uvicorn_log_config_file, "w") as file:
        json.dump(conf_uvicorn, file, indent=4)  # Use indent for pretty formattin
    return uvicorn_log_config_file


def read_options():
    parser = argparse.ArgumentParser(description='This is the server side of the Aansi mimics the AWX/Ansible Tower® workflows from command line.')
    parser.add_argument('--log-dir', dest='log_dir',
                        help='set the parent output logging directory')

    parser.add_argument('--log-level', dest='log_level', default='info', choices=["debug", "info", "warning", "error", "critical"],
                        help='set the logging level. defaults to info')

    parser.add_argument('--socket', dest='socket', default=None,
                        help='unix socket to listen on. defaults to %s' % ipc.DEFAULT_SOCKET_PATH)

    return parser.parse_args()


def serve(socket_path, log_config):
    '''
    Listen on the session socket.

    uvicorn opens a unix socket with mode 0666, so the socket is tightened as
    soon as it is bound. The directory it sits in is the real access gate, but
    a mode nobody outside the group can use is worth having anyway.
    '''
    import uvicorn

    class SessionServer(uvicorn.Server):
        async def startup(self, sockets=None):
            await super().startup(sockets=sockets)
            os.chmod(socket_path, ipc.SOCKET_MODE)

    config = uvicorn.Config(app, uds=socket_path, log_config=log_config)
    SessionServer(config).run()


if __name__ == "__main__":
    cmd_args = read_options()
    log_config = define_logger(cmd_args.log_dir, cmd_args.log_level)

    socket_path = ipc.socket_path(cmd_args.socket)
    ipc.ensure_socket_dir(socket_path, group=os.environ.get(ipc.GROUP_ENV_VAR))
    ipc.remove_stale_socket(socket_path)

    serve(socket_path, log_config)
