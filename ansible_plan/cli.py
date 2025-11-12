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
import logging.handlers
import sys
import argparse
import os
import os.path
from datetime import datetime
import threading
import signal
from rich.console import Console
import httpx
import subprocess
import time

from .ui.stdout import StdoutWorkflowOutput
from .ui.textual import TextualWorkflowOutput
from ansible.cli.arguments import option_helpers as opt_help
from ansible.parsing.splitter import parse_kv

BACKEND_URL = "http://127.0.0.1:8001"

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


def check_and_start_backend(logger, logging_dir):
    try:
        httpx.get(f"{BACKEND_URL}/health")
        logger.info("Backend is already running.")
    except httpx.ConnectError:
        logger.info("Backend not running. Starting it now.")
        project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))

        popen_kwargs = {
            "cwd": project_root,
            "stdout": open(os.path.join(logging_dir, "backend_stdout.log"), "wb"),
            "stderr": open(os.path.join(logging_dir, "backend_stderr.log"), "wb"),
        }

        if os.name == 'nt':
            popen_kwargs['creationflags'] = subprocess.CREATE_NEW_PROCESS_GROUP
        elif os.name == 'posix':
            popen_kwargs['start_new_session'] = True

        process = subprocess.Popen(
            [sys.executable, "-m", "ansible_plan.service", "--log-dir", logging_dir],
            **popen_kwargs
        )

        for _ in range(10):
            try:
                httpx.get(f"{BACKEND_URL}/health")
                logger.info("Backend started successfully.")
                return process
            except httpx.ConnectError:
                time.sleep(1)
        logger.error("Failed to start the backend.")
        sys.exit(1)
    return None


def read_options():
    parser = argparse.ArgumentParser(description='This programs mimics the AWX/Ansible Tower® workflows from command line.')
    parser.add_argument('workflow', type=str, help='Workflow file')

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

    parser.add_argument('--log-dir', dest='log_dir', default='/var/log/ansible/plan',
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

def main():
    os.environ['TERM'] = 'xterm-256color'
    cmd_args = read_options()

    logging_dir = "%s" % cmd_args.log_dir
    if not cmd_args.log_dir_no_info:
        logging_dir += "/%s_%s" % (os.path.basename(cmd_args.workflow), datetime.now().strftime("%Y%m%d_%H%M%S"))

    logger = define_logger(logging_dir, cmd_args.log_level)
    console = Console()

    check_and_start_backend(logger, logging_dir)

    extra_vars = {}
    for single_extra_vars in cmd_args.extra_vars:
        extra_vars.update(parse_kv(single_extra_vars))

    input_templating = {x: y for [x, y] in cmd_args.input_templating}

    start_payload = {
        "workflow_file": os.path.abspath(cmd_args.workflow),
        "extra_vars": extra_vars,
        "input_templating": input_templating,
        "check_mode": cmd_args.check_mode,
        "verbosity": cmd_args.verbosity,
        "start_from_node": cmd_args.start_from_node,
        "end_to_node": cmd_args.end_to_node,
        "skip_nodes": cmd_args.skip_nodes.split(",") if cmd_args.skip_nodes else [],
        "filter_nodes": cmd_args.filter_nodes.split(",") if cmd_args.filter_nodes else [],
        "log_dir": logging_dir,
        "log_level": cmd_args.log_level,
        "verify_only": cmd_args.verify_only,
        "doubtful_mode": cmd_args.doubtful_mode,
    }

    try:
        response = httpx.post(f"{BACKEND_URL}/workflow", json=start_payload, timeout=30)
        response.raise_for_status()
        response_data = response.json()
        if response_data.get("status") == "reconnected":
            console.print(f"Reconnected to running workflow: {cmd_args.workflow}")

    except httpx.HTTPStatusError as e:
        if e.response.status_code == 409:
            detail = e.response.json().get("detail", {})
            message = detail.get("message", "An unknown conflict occurred.")
            running_workflow_path = detail.get("running_workflow_file", "")

            console.print(f"[bold red]Error:[/bold red] {message}")

            if running_workflow_path:
                y_or_n = console.input(f"Do you want to connect to the running workflow at '{running_workflow_path}'? [y/n] ")
                if y_or_n.lower() == 'y':
                    cmd_args.workflow = running_workflow_path
                    console.print(f"Connecting to existing workflow: {cmd_args.workflow}")
                else:
                    sys.exit(0)
            else:
                sys.exit(1)
        else:
            logger.error(f"Failed to start workflow: {e}")
            print(f"Failed to start workflow: {e}", file=sys.stderr)
            if hasattr(e, 'response') and e.response:
                print(e.response.text, file=sys.stderr)
            sys.exit(1)
    except httpx.ConnectError as e:
        logger.error(f"Failed to start workflow: {e}")
        print(f"Failed to start workflow: {e}", file=sys.stderr)
        sys.exit(1)


    if cmd_args.mode == 'textual':
        output = TextualWorkflowOutput(
            backend_url=BACKEND_URL,
            event=threading.Event(),
            logging_dir=logging_dir,
            log_level=cmd_args.log_level,
            cmd_args=cmd_args
        )
        output.run()

        try:
            response = httpx.get(f"{BACKEND_URL}/workflow")
            response.raise_for_status()
            status = response.json().get("status")
            if status == "running":
                console.print("\nDetaching from workflow. The backend will continue to run.")
                console.print("To re-attach, run the same command again.")
        except (httpx.ConnectError, httpx.HTTPStatusError):
            pass
    else:
        stdout_thread = StdoutWorkflowOutput(
            backend_url=BACKEND_URL,
            event=threading.Event(),
            logging_dir=logging_dir,
            log_level=cmd_args.log_level,
            cmd_args=cmd_args
        )
        stdout_thread.start()

        def signal_handler(sig, frame):
            console.print("\nDetaching from workflow. The backend will continue to run.")
            console.print("To re-attach, run the same command again.")
            stdout_thread.event.set()
            # wait for the thread to finish
            stdout_thread.join()
            sys.exit(0)

        signal.signal(signal.SIGINT, signal_handler)
        stdout_thread.join()

    # Shutdown logic
    try:
        response = httpx.get(f"{BACKEND_URL}/workflow")
        response.raise_for_status()
        status = response.json().get("status")
        if status != "running":
            logger.info("Workflow finished. Shutting down backend.")
            httpx.post(f"{BACKEND_URL}/shutdown")
    except (httpx.ConnectError, httpx.HTTPStatusError) as e:
        logger.warning(f"Could not get workflow status or shutdown backend: {e}")


if __name__ == "__main__":
    main()
