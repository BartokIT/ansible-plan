import os
import signal
import sys
import threading
import time
from datetime import datetime
from typing import Optional

import jinja2
from fastapi import FastAPI, BackgroundTasks, HTTPException

from .exceptions import (
    AnsibleWorkflowLoadingError,
    AnsibleWorkflowValidationError,
    AnsibleWorkflowVaultScript,
    AnsibleWorkflowYAMLNotValid,
)
from .loader import WorkflowYamlLoader
from .models import WorkflowStartRequest, PlaybookNodeInfo, BlockNodeInfo
from .workflow import AnsibleWorkflow, WorkflowStatus

app = FastAPI()


class WorkflowAPI:
    def __init__(self):
        self.workflow_lock = threading.Lock()
        self.current_workflow: Optional[AnsibleWorkflow] = None

    async def start_workflow(self, request: WorkflowStartRequest, background_tasks: BackgroundTasks):
        with self.workflow_lock:
            if self.current_workflow and self.current_workflow.get_running_status() in [WorkflowStatus.RUNNING, WorkflowStatus.ENDED]:
                if self.current_workflow.get_workflow_file() == request.workflow_file:
                    return {"status": "reconnected"}
                else:
                    raise HTTPException(status_code=409, detail={
                        "message": "A different workflow is already running",
                        "running_workflow_file": self.current_workflow.get_workflow_file()
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
                self.current_workflow = aw
            except (
                AnsibleWorkflowVaultScript,
                AnsibleWorkflowValidationError,
                AnsibleWorkflowYAMLNotValid,
                AnsibleWorkflowLoadingError,
                jinja2.exceptions.UndefinedError,
            ) as e:
                raise HTTPException(status_code=400, detail=str(e))

            if request.filter_nodes:
                aw.set_filtered_nodes(request.filter_nodes)
            if request.skip_nodes:
                aw.set_skipped_nodes(request.skip_nodes)

            start_node = request.start_from_node if request.start_from_node else '_s'
            end_node = request.end_to_node if request.end_to_node else '_e'

            background_tasks.add_task(aw.run, start_node=start_node, end_node=end_node, verify_only=request.verify_only)

        return {"status": WorkflowStatus.RUNNING}

    def get_workflow_status(self):
        with self.workflow_lock:
            if not self.current_workflow:
                return {"status": WorkflowStatus.NOT_STARTED}
            return {"status": self.current_workflow.get_running_status()}

    def get_workflow_nodes(self):
        with self.workflow_lock:
            if not self.current_workflow:
                return []

            nodes_data = [
                self.current_workflow.get_node_object(node_id).to_info()
                for node_id in self.current_workflow.get_nodes()
                if hasattr(self.current_workflow.get_node_object(node_id), 'to_info')
            ]
            return nodes_data

    def get_workflow_graph(self):
        with self.workflow_lock:
            if not self.current_workflow:
                raise HTTPException(status_code=404, detail="Workflow not found.")
            return {"edges": self.current_workflow.get_original_graph_edges()}

    def get_node_stdout(self, node_id: str):
        with self.workflow_lock:
            if not self.current_workflow:
                raise HTTPException(status_code=404, detail="Workflow not found.")

            logging_dir = self.current_workflow.get_logging_dir()
            node_obj = self.current_workflow.get_node_object(node_id)
            if not isinstance(node_obj, PNode):
                raise HTTPException(status_code=404, detail="Node is not a playbook node.")

            ident = getattr(node_obj, 'ident', node_id)
            stdout_path = os.path.join(logging_dir, ident, "stdout")

            if not os.path.exists(stdout_path):
                return {"stdout": ""}

            with open(stdout_path, "r") as f:
                return {"stdout": f.read()}

    def stop_workflow(self):
        with self.workflow_lock:
            if not self.current_workflow or self.current_workflow.get_running_status() != WorkflowStatus.RUNNING:
                raise HTTPException(status_code=404, detail="No running workflow to stop.")
            self.current_workflow.stop()
        return {"message": "Workflow stopping."}

    def restart_node(self, node_id: str):
        with self.workflow_lock:
            if not self.current_workflow:
                raise HTTPException(status_code=404, detail="Workflow not found.")

            self.current_workflow.restart_failed_node(node_id)

        return {"message": f"Node {node_id} restarting."}

    def skip_node(self, node_id: str):
        with self.workflow_lock:
            if not self.current_workflow:
                raise HTTPException(status_code=404, detail="Workflow not found.")

            node_obj = self.current_workflow.get_node_object(node_id)
            node_obj.set_skipped()
            # We also need to add it to the running nodes so the workflow progresses
            self.current_workflow.add_running_node(node_id)

        return {"message": f"Node {node_id} skipped."}

    def shutdown(self):
        with self.workflow_lock:
            if self.current_workflow:
                if self.current_workflow.get_running_status() == WorkflowStatus.RUNNING:
                    raise HTTPException(status_code=409, detail="Cannot shutdown while a workflow is running.")
                # Tell the workflow thread to stop
                self.current_workflow.stop()
                # Give the thread a moment to stop
                time.sleep(1.5)

            # This is a simple way to shutdown for this app.
            # In a real production app, a more graceful shutdown mechanism would be needed.
            os.kill(os.getpid(), signal.SIGTERM)

        return {"message": "Shutting down."}

    def health_check(self):
        return {"status": "ok"}


workflow_api = WorkflowAPI()

app.add_api_route("/workflow", workflow_api.start_workflow, methods=["POST"])
app.add_api_route("/workflow", workflow_api.get_workflow_status, methods=["GET"])
app.add_api_route("/workflow/nodes", workflow_api.get_workflow_nodes, methods=["GET"])
app.add_api_route("/workflow/graph", workflow_api.get_workflow_graph, methods=["GET"])
app.add_api_route("/workflow/node/{node_id}/stdout", workflow_api.get_node_stdout, methods=["GET"])
app.add_api_route("/workflow/stop", workflow_api.stop_workflow, methods=["POST"])
app.add_api_route("/workflow/node/{node_id}/restart", workflow_api.restart_node, methods=["POST"])
app.add_api_route("/workflow/node/{node_id}/skip", workflow_api.skip_node, methods=["POST"])
app.add_api_route("/shutdown", workflow_api.shutdown, methods=["POST"])
app.add_api_route("/health", workflow_api.health_check, methods=["GET"])


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8001, log_level="info")
