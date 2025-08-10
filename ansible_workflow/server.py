import Pyro5.api
import Pyro5.nameserver
import sys
import threading
import os
import time
from workflow import AnsibleWorkflow
from datetime import datetime

def start_server(workflow_path, inventory_path, log_dir_base):
    # The logging dir needs to be created, similar to the original __main__.py
    if not os.path.exists(log_dir_base):
        os.makedirs(log_dir_base)

    logging_dir = "%s/%s_%s" % (log_dir_base, os.path.basename(workflow_path), datetime.now().strftime("%Y%m%d_%H%M%S"))

    print("Starting Pyro name server in background...")
    ns_thread = threading.Thread(target=Pyro5.nameserver.start_ns_loop)
    ns_thread.daemon = True
    ns_thread.start()
    time.sleep(1) # Give the name server a moment to start

    print(f"Starting workflow server for: {workflow_path}")
    print(f"Logging to: {logging_dir}")

    # We need a daemon to listen for requests
    daemon = Pyro5.api.Daemon()

    # Find a suitable name server
    ns = Pyro5.api.locate_ns()

    # Create an instance of the workflow
    ansible_workflow = AnsibleWorkflow(workflow=workflow_path,
                                       inventory=inventory_path,
                                       logging_dir=logging_dir)

    # Register the workflow instance as a Pyro object
    uri = daemon.register(ansible_workflow)

    # Register the object with a name in the name server
    # Using a fixed name for simplicity. In a real-world scenario, you might
    # want to use a unique name per workflow.
    ns.register("ansible.workflow", uri)

    print("Server ready. Object uri =", uri)
    print("Registered as 'ansible.workflow' in the name server.")

    # The daemon will run in the main thread.
    # The client will call the run() method on the workflow object,
    # which will start a new thread for the workflow execution.
    try:
        daemon.requestLoop()
    except KeyboardInterrupt:
        print("Shutting down server.")
    finally:
        ns.remove("ansible.workflow")
        daemon.shutdown()

def main():
    # This main is for standalone execution for testing
    if len(sys.argv) < 3:
        print("Usage: python -m ansible-workflow.server <workflow_file> <inventory_file>")
        sys.exit(1)

    workflow_path = sys.argv[1]
    inventory_path = sys.argv[2]
    log_dir = "/tmp/ansible-workflows"
    start_server(workflow_path, inventory_path, log_dir)

if __name__ == "__main__":
    main()
