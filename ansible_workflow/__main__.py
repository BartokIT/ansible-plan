import argparse
import os
import sys
import time
import daemon
import Pyro5.api
import Pyro5.errors
from .server import start_server, PYRO_CONTROLLER_NAME
from .client import main as client_main

def check_file_existence(workflow, inventories):
    error = False
    if not os.path.exists(workflow):
        print(f"The workflow file {workflow} doesn't exist. Please provide a correct file", file=sys.stderr)
        error = True

    if inventories:
        inventory_list = inventories.split(',')
        for inventory in inventory_list:
            if not os.path.exists(inventory):
                print(f"The inventory file {inventory} doesn't exist. Please provide a correct file", file=sys.stderr)
                error = True
    if error:
        sys.exit(1)

def is_server_running():
    """Check if the Pyro5 server is running by trying to connect."""
    try:
        # Use a timeout to avoid waiting forever
        with Pyro5.api.Proxy(f"PYRONAME:{PYRO_CONTROLLER_NAME}") as proxy:
            proxy._pyroTimeout = 2
            proxy._pyroBind()
        return True
    except Pyro5.errors.CommunicationError:
        # This is expected if the server is not running
        return False
    except Pyro5.errors.NamingError:
        # This is expected if the name server is not running (and thus the server is not)
        return False

def launch_daemonized_server():
    """Launch the server process as a background daemon."""
    print("Backend server not found. Starting it now...")
    # Using python-daemon to fork the process
    # We need to ensure that stdout/stderr are handled, e.g., redirected to a log file.
    # For simplicity, we'll let the daemon library handle it.
    context = daemon.DaemonContext(
        working_directory=os.getcwd(),
        # For debugging, you might want to redirect stdout/stderr to files
        # stdout=open('daemon_stdout.log', 'w+'),
        # stderr=open('daemon_stderr.log', 'w+'),
    )

    with context:
        start_server()

def main():
    parser = argparse.ArgumentParser(description='Ansible Workflow runner.')
    parser.add_argument('workflow', type=str, help='Workflow file to load and run.')
    parser.add_argument('-i', '--inventory', dest='inventory', required=True,
                               help='Specify inventory host path or comma separated host list.')
    args = parser.parse_args()

    check_file_existence(args.workflow, args.inventory)

    if not is_server_running():
        launch_daemonized_server()
        # Give the server a moment to start up and register itself
        time.sleep(3)
        if not is_server_running():
            print("Failed to start the backend server. Please check logs for details.", file=sys.stderr)
            sys.exit(1)
        print("Backend server started successfully.")

    # Now that we know the server is running, launch the client
    client_main(args.workflow, args.inventory)


if __name__ == "__main__":
    main()
