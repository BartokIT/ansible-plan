import asyncio
import logging
import os
import threading
import time
import signal
from typing import Optional, Dict, Any, List

from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from fastapi.responses import JSONResponse
import networkx as nx

from ansible_workflow.workflow import AnsibleWorkflow, WorkflowStatus, NodeStatus
from ansible_workflow.loader import WorkflowYamlLoader
from ansible_workflow.exceptions import (AnsibleWorkflowLoadingError, AnsibleWorkflowValidationError,
                                         AnsibleWorkflowVaultScript, AnsibleWorkflowYAMLNotValid)
import jinja2

app = FastAPI()

current_workflow: Optional[AnsibleWorkflow] = None
workflow_lock = threading.Lock()

class WorkflowRequest(BaseModel):
    workflow_file: str
    extra_vars: Dict[str, Any] = {}
    input_templating: Dict[str, str] = {}
    check_mode: bool = False
    verbosity: int = 0
    start_from_node: str = "_s"
    end_to_node: str = "_e"
    filter_nodes: List[str] = []
    skip_nodes: List[str] = []
    log_dir: str = "/var/log/ansible/workflows"
    log_level: str = "info"


def run_workflow_thread(workflow: AnsibleWorkflow, start_node: str, end_node: str):
    try:
        workflow.run(start_node=start_node, end_node=end_node)
    except Exception as e:
        # Log the exception
        logging.getLogger("backend").error(f"Workflow execution failed: {e}")
        pass
    finally:
        if os.environ.get("TERMINATE_WHEN_DONE") == "1":
            # Give the frontend a moment to get the final status
            time.sleep(5)
            os.kill(os.getpid(), signal.SIGINT)

@app.post("/workflow")
async def start_workflow(request: WorkflowRequest):
    global current_workflow
    with workflow_lock:
        if current_workflow and current_workflow.get_running_status() == WorkflowStatus.RUNNING:
            raise HTTPException(status_code=409, detail="A workflow is already running.")

        try:
            # Note: This is a simplified logger for the backend itself.
            # The workflow will have its own logger configured by the loader.
            logging.basicConfig(level=request.log_level.upper())
            backend_logger = logging.getLogger("backend")

            if not os.path.exists(request.workflow_file):
                 raise HTTPException(status_code=404, detail=f"Workflow file not found: {request.workflow_file}")

            wl = WorkflowYamlLoader(
                request.workflow_file,
                request.log_dir,
                request.log_level,
                request.input_templating,
                request.check_mode,
                request.verbosity
            )
            aw = wl.parse(request.extra_vars)
            current_workflow = aw

            if request.filter_nodes:
                aw.set_filtered_nodes(request.filter_nodes)
            if request.skip_nodes:
                aw.set_skipped_nodes(request.skip_nodes)

            thread = threading.Thread(
                target=run_workflow_thread,
                args=(aw, request.start_from_node, request.end_to_node)
            )
            thread.daemon = True
            thread.start()

            return {"message": "Workflow started."}

        except (AnsibleWorkflowLoadingError, AnsibleWorkflowValidationError,
                AnsibleWorkflowVaultScript, AnsibleWorkflowYAMLNotValid,
                jinja2.exceptions.UndefinedError) as e:
            backend_logger.error(f"Failed to start workflow: {e}")
            raise HTTPException(status_code=400, detail=str(e))


@app.get("/workflow")
async def get_workflow_status():
    if not current_workflow:
        return {"status": "No workflow is running or has been run."}

    status = current_workflow.get_running_status().value
    nodes = {}
    if current_workflow.get_node_datas():
        for node_id, data in current_workflow.get_node_datas().items():
            if 'object' in data:
                node_obj = data['object']
                nodes[node_id] = {
                    "status": node_obj.get_status().value,
                    "type": node_obj.get_type(),
                    "telemetry": node_obj.get_telemetry()
                }

    return {"status": status, "nodes": nodes}


@app.delete("/workflow")
async def stop_workflow():
    if not current_workflow or current_workflow.get_running_status() != WorkflowStatus.RUNNING:
        raise HTTPException(status_code=404, detail="No running workflow to stop.")

    current_workflow.stop()
    return {"message": "Workflow stopping."}


@app.get("/workflow/graph")
async def get_workflow_graph():
    if not current_workflow:
        raise HTTPException(status_code=404, detail="No workflow is running or has been run.")

    graph = current_workflow.get_graph()
    return JSONResponse(content=nx.readwrite.json_graph.node_link_data(graph))


@app.get("/status")
async def get_status():
    return {"status": "ok"}
