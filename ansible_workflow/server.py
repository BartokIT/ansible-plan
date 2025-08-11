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
        """Loads a workflow if one is not already present."""
        if self.workflow:
            # A workflow is already loaded, do nothing.
            # The client will now see the state of this existing workflow.
            return f"Reconnected to existing workflow. Status: {self.workflow.get_workflow_status()}"

        # No workflow has been loaded yet for the lifetime of this server. Load it now.
        print(f"First client connected. Loading workflow from: {workflow_path}")
        logging_dir = "%s/%s_%s" % (self.log_dir_base, os.path.basename(workflow_path), datetime.now().strftime("%Y%m%d_%H%M%S"))

        try:
            self.workflow = AnsibleWorkflow(
                workflow=workflow_path,
                inventory=inventory_path,
                logging_dir=logging_dir
            )
            return "Workflow loaded successfully for the first time."
        except Exception as e:
            # If loading fails, the server remains in a state with no workflow.
            self.workflow = None
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

    def get_node_details(self, node_id):
        if not self.workflow:
            return {}
        # The data dict in workflow contains all info
        node_data = self.workflow.get_node_datas().get(node_id, {})
        # We only need to return the serializable parts
        return {
            'status': node_data.get('status'),
            'type': node_data.get('type'),
            'details': node_data.get('details'),
            'started': node_data.get('started'),
            'ended': node_data.get('ended'),
        }

    def restart_workflow(self):
        """Resets the controller to a state with no workflow loaded."""
        if self.workflow and self.workflow.get_workflow_status() in ['running']:
            return "Cannot restart while workflow is running."

        print("Restarting workflow. Clearing current state.")
        self.workflow = None
        return "Workflow cleared. Ready to load a new one."

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
