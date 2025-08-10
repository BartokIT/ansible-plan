import Pyro5.api
import Pyro5.nameserver
import sys
import threading
import os
import time
from .workflow import AnsibleWorkflow
from datetime import datetime

PYRO_CONTROLLER_NAME = "ansible.workflow.controller"

@Pyro5.api.expose
class WorkflowController:
    def __init__(self):
        self.workflow = None
        self.daemon = None # Set by start_server
        self.log_dir_base = "/tmp/ansible-workflows"
        if not os.path.exists(self.log_dir_base):
            os.makedirs(self.log_dir_base)

    def load_workflow(self, workflow_path, inventory_path):
        """Loads a new workflow, but only if one is not already active."""
        if self.workflow and self.workflow.get_workflow_status() not in ['ended', 'stopped', 'failed']:
             # If a workflow is active, just return its status
            return f"Workflow already active with status: {self.workflow.get_workflow_status()}"

        logging_dir = "%s/%s_%s" % (self.log_dir_base, os.path.basename(workflow_path), datetime.now().strftime("%Y%m%d_%H%M%S"))

        try:
            self.workflow = AnsibleWorkflow(
                workflow=workflow_path,
                inventory=inventory_path,
                logging_dir=logging_dir
            )
            return "Workflow loaded successfully."
        except Exception as e:
            return f"Error loading workflow: {e}"

    def get_workflow_status(self):
        if not self.workflow:
            return "no_workflow_loaded"
        return self.workflow.get_workflow_status()

    def get_input_data(self):
        if not self.workflow:
            return []
        return self.workflow.get_input_data()

    def get_nodes_status(self):
        if not self.workflow:
            return {}
        return self.workflow.get_nodes_status()

    def run(self):
        if not self.workflow:
            return "No workflow loaded."
        return self.workflow.run()

    def stop(self):
        if not self.workflow:
            return "No workflow loaded."
        self.workflow.stop()
        return "Stop signal sent."

    def tail_playbook_output(self, node_id, offset=0):
        if not self.workflow:
            return "No workflow loaded.", 0
        return self.workflow.tail_playbook_output(node_id, offset)

    @Pyro5.api.oneway
    def request_shutdown(self):
        """Request the server to shut down if the workflow is complete."""
        if self.workflow and self.workflow.get_workflow_status() in ['ended', 'failed', 'stopped']:
            print("Workflow finished and client exited. Shutting down server.")
            # Run shutdown in a new thread to allow the oneway call to complete
            threading.Thread(target=self.daemon.shutdown, daemon=True).start()


def start_server():
    print("Starting Pyro name server in background...")
    ns_thread = threading.Thread(target=Pyro5.nameserver.start_ns_loop)
    ns_thread.daemon = True
    ns_thread.start()
    time.sleep(1)

    print("Starting workflow controller server...")
    daemon = Pyro5.api.Daemon()
    ns = Pyro5.api.locate_ns()

    controller = WorkflowController()
    controller.daemon = daemon  # Give the controller a reference to the daemon
    uri = daemon.register(controller)
    ns.register(PYRO_CONTROLLER_NAME, uri)

    print(f"Server ready. Controller registered as '{PYRO_CONTROLLER_NAME}'")

    try:
        daemon.requestLoop()
    except KeyboardInterrupt:
        print("Shutting down server.")
    finally:
        ns.remove(PYRO_CONTROLLER_NAME)
        daemon.shutdown()

# The main entry point is now in __main__.py
# The old main() function is removed to avoid confusion.
