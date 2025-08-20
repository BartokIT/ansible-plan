import os
print(f"Backend CWD: {os.getcwd()}")
import signal
import sys
import threading
import time
from datetime import datetime
from fastapi import FastAPI, HTTPException, BackgroundTasks
from pydantic import BaseModel, Field
from typing import Optional, Dict, List

from .core.loader import WorkflowYamlLoader
from .core.engine import AnsibleWorkflow
from .core.models import NodeStatus, WorkflowStatus, PNode
from .core.exceptions import (
    AnsibleWorkflowLoadingError,
    AnsibleWorkflowValidationError,
    AnsibleWorkflowVaultScript,
    AnsibleWorkflowYAMLNotValid,
    AnsibleWorkflowPlaybookNodeCheck,
)
import jinja2

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
    log_dir_no_info: bool = False
    log_level: str = "info"
    verify_only: bool = False


@app.post("/workflow")
async def start_workflow(request: WorkflowStartRequest, background_tasks: BackgroundTasks):
    global current_workflow
    with workflow_lock:
        if current_workflow and current_workflow.get_running_status() in [WorkflowStatus.RUNNING, WorkflowStatus.ENDED]:
            if current_workflow.get_workflow_file() == request.workflow_file:
                return {"status": "reconnected"}
            else:
                raise HTTPException(status_code=409, detail={
                    "message": "A different workflow is already running",
                    "running_workflow_file": current_workflow.get_workflow_file()
                })

        logging_dir = "%s" % request.log_dir
        if not request.log_dir_no_info:
            logging_dir += "/%s_%s" % (os.path.basename(request.workflow_file), datetime.now().strftime("%Y%m%d_%H%M%S"))

        try:
            loader = WorkflowYamlLoader(
                request.workflow_file,
                logging_dir,
                request.log_level,
                request.input_templating,
                request.check_mode,
                request.verbosity,
            )
            aw = loader.parse(request.extra_vars)
            current_workflow = aw
        except (
            AnsibleWorkflowLoadingError,
            jinja2.exceptions.UndefinedError,
        ) as e:
            aw = AnsibleWorkflow(
                workflow_file=request.workflow_file,
                logging_dir=logging_dir,
                log_level=request.log_level,
            )
            aw.add_validation_error(str(e))
            aw.set_status(WorkflowStatus.FAILED)
            current_workflow = aw
            return {"status": WorkflowStatus.FAILED, "validation_errors": [str(e)]}

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
        for node_id in current_workflow.get_nodes():
            node_obj = current_workflow.get_node_object(node_id)
            status = node_obj.get_status()
            node_info = {
                "id": node_obj.get_id(),
                "status": status.value if hasattr(status, 'value') else status,
                "type": node_obj.get_type(),
            }
            if isinstance(node_obj, PNode):
                node_info.update({
                    "playbook": node_obj.get_playbook(),
                    "description": node_obj.get_description(),
                    "reference": node_obj.get_reference(),
                })
                node_info.update(node_obj.get_telemetry())
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
def stop_workflow():
    with workflow_lock:
        if not current_workflow or current_workflow.get_running_status() != WorkflowStatus.RUNNING:
            raise HTTPException(status_code=404, detail="No running workflow to stop.")
        current_workflow.stop()
    return {"message": "Workflow stopping."}


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

        node_obj = current_workflow.get_node_object(node_id)
        node_obj.set_skipped()
        # We also need to add it to the running nodes so the workflow progresses
        current_workflow.add_running_node(node_id)

    return {"message": f"Node {node_id} skipped."}


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


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8001, log_level="info")
