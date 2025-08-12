#!/usr/bin/env python3
# -*- coding: utf-8 -*-
#
# Copyright 2021 BartokIT
# Ansible® is a registered trademark of Red Hat, Inc. in the United States and other countries.
#
#
# This program is free software; you can redistribute it and/or modify
# it under the terms of the GNU Affero General Public License v3.0 as published by
# the Free Software Foundation.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU Affero General Public License for more details.
#
# You should have received a copy of the  GNU Affero General Public License v3.0
# along with this program; if not, see <https://www.gnu.org/licenses/agpl.html/>.

import logging
import sys
import argparse
import os.path
from datetime import datetime
import threading
import jinja2
import signal
from rich.console import Console
import requests
import subprocess
import time
import json
import uvicorn

from .exceptions import (AnsibleWorkflowLoadingError, AnsibleWorkflowValidationError, AnsibleWorkflowVaultScript,
                         ExitCodes, AnsibleWorkflowYAMLNotValid)
from .loader import WorkflowYamlLoader
from .output import StdoutWorkflowOutput, PngDrawflowOutput, TextualWorkflowOutput
from ansible.cli.arguments import option_helpers as opt_help
from ansible.parsing.splitter import parse_kv

BACKEND_HOST = "127.0.0.1"
BACKEND_PORT = 8088
BACKEND_URL = f"http://{BACKEND_HOST}:{BACKEND_PORT}"

def define_logger(logging_dir, level):
    logger_file_path = os.path.join(logging_dir, 'main.log')
    if not os.path.exists(os.path.dirname(logger_file_path)):
        os.makedirs(os.path.dirname(logger_file_path))

    logger = logging.getLogger('main')
    if level:
        logger.setLevel(getattr(logging, level.upper()))
    logger_handler = logging.handlers.TimedRotatingFileHandler(
        logger_file_path,
        when='d',
        backupCount=3,
        encoding='utf8'
    )
    logger_handler.setFormatter(logging.Formatter('%(asctime)s [%(levelname)s] %(name)s: %(message)s'))
    logger.addHandler(logger_handler)
    return logger


def read_options():
    parser = argparse.ArgumentParser(description='This programs mimics the AWX/Ansible Tower® workflows from command line.')
    parser.add_argument('workflow', type=str, nargs='?', default=None, help='Workflow file')

    # Backend commands
    parser.add_argument('--start-backend', action='store_true', help='Start the backend server.')
    parser.add_argument('--terminate-when-done', action='store_true', help='Terminate backend when workflow is done.')


    # influences workflow execution
    group = parser.add_mutually_exclusive_group()

    group.add_argument('--execute-nodes', dest='filter_nodes', default="",
                        help='filter nodes to be executed inside a workflow, executing only the selected (comma separated).')

    group.add_argument('--skip-nodes', dest='skip_nodes', default="",
                        help='Filter nodes to be executed inside a workflow, skipping the selected (comma separated)')

    parser.add_argument('-sn', '--start-from-node', dest='start_from_node', default="",
                        help='Start the execution of the workflow from the specified node')

    parser.add_argument('-en', '--end-to-node', dest='end_to_node', default="",
                        help='Stop the execution of the workflow to the specified node')

    parser.add_argument('-vo', '--verify-only', dest='verify_only', action='store_true',
                        help='Perform only the verification of the workflow content')

    parser.add_argument('-c', '--check', dest='check_mode', action='store_true',
                        help='Launch all playbook in check mode')

    parser.add_argument('-v', dest='verbosity', action='count', default=0,
                        help='The logging level of the playbook')

    parser.add_argument('-nir', '--no-interactive-retry', dest='interactive_retry', action='store_false',
                        help='Avoid interactive retry of job')

    parser.add_argument('--doubtful-mode', dest='doubtful_mode', action='store_true',
                        help='Ask for each node if should be started or skipped')

    # add extra vars parameter from ansible library
    opt_help.add_runtask_options(parser)

    # influences the output
    parser.add_argument('--mode', default='stdout', choices=["stdout", "textual"],
                        help='Render the progress using textual or stdout.')

    parser.add_argument('-d', '--draw', dest='draw_png', action='store_true',
                        help='Output also a PNG of the graph inside the log folder')

    parser.add_argument('-dpi', '--draw-dpi', dest='draw_dpi', default=72, type=int,
                        help='Choose the dpi of the draw graph')

    parser.add_argument('-size', '--draw-size', dest='draw_size', default=10, type=int,
                        help='Choose the size of the draw graph')

    parser.add_argument('--log-dir', dest='log_dir', default='logs',
                        help='set the parent output logging directory. defaults to logs/[workflow name]-[execution time]')

    parser.add_argument('--log-dir-no-info', dest='log_dir_no_info', action='store_true',
                        help='Does not add the workflow name and the execution time to the log dir name')

    parser.add_argument('--log-level', dest='log_level', default='info', choices=["debug", "info", "warning", "error", "critical"],
                        help='set the logging level. defaults to info')

    # loader input
    parser.add_argument('-it', '--input-templating', dest='input_templating', default=[], action='append', type=keyvalue,
                        help='Input variable for the templating -it key1=value1 -it key2=value2')

    return parser.parse_args()

def keyvalue(value):
    if '=' not in value:
        raise Exception('Key value malformatted: key=value, missing the "="')
    return value.split('=')

def is_backend_running():
    try:
        response = requests.get(f"{BACKEND_URL}/status")
        return response.status_code == 200
    except requests.ConnectionError:
        return False

def start_backend(terminate_when_done=False):
    cmd = [sys.executable, "-m", "ansible_workflow", "--start-backend"]
    if terminate_when_done:
        cmd.append("--terminate-when-done")

    subprocess.Popen(cmd, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)

    # Wait for backend to start
    for _ in range(10):
        if is_backend_running():
            return True
        time.sleep(0.5)
    return False

def main():
    cmd_args = read_options()

    if cmd_args.start_backend:
        # This is the backend process
        # I need to find a way to pass --terminate-when-done to the backend app
        # For now, I'll handle it in the backend code directly.
        os.environ["TERMINATE_WHEN_DONE"] = "1" if cmd_args.terminate_when_done else "0"
        uvicorn.run("ansible_workflow.backend:app", host=BACKEND_HOST, port=BACKEND_PORT, log_level="info")
        return

    if not cmd_args.workflow:
        print("Error: workflow file argument is required.", file=sys.stderr)
        sys.exit(ExitCodes.WORKFLOW_FILE_TYPE_NOT_SUPPORTED.value)

    # This is the frontend CLI
    if not is_backend_running():
        print("Backend not running. Starting it now...")
        if not start_backend(terminate_when_done=True):
            print("Error: Could not start backend.", file=sys.stderr)
            sys.exit(1)
        print("Backend started.")

    extra_vars = {}
    for single_extra_vars in cmd_args.extra_vars:
        extra_vars.update(parse_kv(single_extra_vars))
    input_templating = {x: y for [x, y] in cmd_args.input_templating}

    # calculate the logging directory
    logging_dir = "%s" % cmd_args.log_dir
    if not cmd_args.log_dir_no_info:
        logging_dir += "/%s_%s" % (os.path.basename(cmd_args.workflow), datetime.now().strftime("%Y%m%d_%H%M%S"))

    payload = {
        "workflow_file": os.path.abspath(cmd_args.workflow),
        "extra_vars": extra_vars,
        "input_templating": input_templating,
        "check_mode": cmd_args.check_mode,
        "verbosity": cmd_args.verbosity,
        "start_from_node": cmd_args.start_from_node if cmd_args.start_from_node else '_s',
        "end_to_node": cmd_args.end_to_node if cmd_args.end_to_node else '_e',
        "filter_nodes": cmd_args.filter_nodes.split(",") if cmd_args.filter_nodes else [],
        "skip_nodes": cmd_args.skip_nodes.split(",") if cmd_args.skip_nodes else [],
        "log_dir": logging_dir,
        "log_level": cmd_args.log_level,
    }

    try:
        response = requests.post(f"{BACKEND_URL}/workflow", json=payload)
        response.raise_for_status()
    except requests.exceptions.RequestException as e:
        print(f"Error starting workflow: {e}", file=sys.stderr)
        if e.response:
            print(f"Backend response: {e.response.text}", file=sys.stderr)
        sys.exit(1)

    print("Workflow started. Polling for status...")

    def signal_handler(sig, frame):
        console = Console()
        y_or_n = console.input('Do you want to stop the workflow? [y/n] ')
        if y_or_n.lower() == 'y':
            try:
                requests.delete(f"{BACKEND_URL}/workflow")
                console.print("Workflow stopping request sent.")
            except requests.exceptions.RequestException as e:
                console.print(f"Error stopping workflow: {e}")

    signal.signal(signal.SIGINT, signal_handler)

    while True:
        try:
            response = requests.get(f"{BACKEND_URL}/workflow")
            response.raise_for_status()
            status_data = response.json()

            # Simple stdout display logic
            console = Console()
            console.clear()
            console.print(f"Workflow Status: {status_data['status']}")
            console.print("-" * 20)
            if "nodes" in status_data:
                for node_id, node_info in status_data["nodes"].items():
                    console.print(f"Node: {node_id:<20} Status: {node_info['status']:<15} Type: {node_info['type']}")

            if status_data["status"] in ["ended", "failed"]:
                print(f"Workflow finished with status: {status_data['status']}")
                break

            time.sleep(2)

        except requests.exceptions.RequestException as e:
            print(f"Error polling workflow status: {e}", file=sys.stderr)
            break
        except KeyboardInterrupt:
            # This is handled by the signal handler, but as a fallback
            break


if __name__ == "__main__":
    main()
