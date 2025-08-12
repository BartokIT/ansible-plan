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
from .exceptions import (AnsibleWorkflowLoadingError, AnsibleWorkflowValidationError, AnsibleWorkflowVaultScript,
                         ExitCodes, AnsibleWorkflowYAMLNotValid)
from .loader import WorkflowYamlLoader
from .output import CursesWorkflowOutput, StdoutWorkflowOutput, PngDrawflowOutput, TextualWorkflowOutput
from ansible.cli.arguments import option_helpers as opt_help
from ansible.parsing.splitter import parse_kv


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

    parser.add_argument('--log-dir', dest='log_dir', default='/var/log/ansible/workflows',
                        help='set the parent output logging directory. defaults to /var/log/ansible/workflows/[workflow name]-[execution time]')

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
    cmd_args = read_options()
    extra_vars = {}
    for single_extra_vars in cmd_args.extra_vars:
        extra_vars.update(parse_kv(single_extra_vars))
    input_templating = {x: y for [x, y] in cmd_args.input_templating}

    # calculate the logging directory
    logging_dir = "%s" % cmd_args.log_dir
    if not cmd_args.log_dir_no_info:
        logging_dir += "/%s_%s" % (os.path.basename(cmd_args.workflow), datetime.now().strftime("%Y%m%d_%H%M%S"))

    logger = define_logger(logging_dir, cmd_args.log_level)
    aw = None
    if cmd_args.workflow.endswith('.yml'):
        try:
            wl = WorkflowYamlLoader(cmd_args.workflow, logging_dir, cmd_args.log_level, input_templating, cmd_args.check_mode, cmd_args.verbosity)
            aw = wl.parse(extra_vars)
        except AnsibleWorkflowVaultScript as vserr:
            print("Vault script error.\n%s" % vserr, file=sys.stderr)
            sys.exit(ExitCodes.VAULT_SCRIPT_ABSENT.value)
        except AnsibleWorkflowValidationError as valerr:
            logger.fatal("Impossible to parse the workflow. See loader log for details")
            print("Wrong workflow format.\n%s" % str(valerr), file=sys.stderr)
            sys.exit(ExitCodes.VALIDATION_ERROR.value)
        except AnsibleWorkflowYAMLNotValid as yerr:
            logger.fatal("Impossible to parse the workflow. See loader log for details")
            print("Wrong YAML format.\n%s" % str(yerr), file=sys.stderr)
            sys.exit(ExitCodes.YAML_NOT_VALID.value)
        except AnsibleWorkflowLoadingError as lerr:
            logger.fatal("Impossible to parse the workflow. See loader log for details")
            print("Wrong YAML format.\n%s" % str(lerr), file=sys.stderr)
            sys.exit(ExitCodes.WORKFLOW_NOT_VALID.value)
        except jinja2.exceptions.UndefinedError as jerr:
            logger.fatal(f"Impossible to parse the workflow. Templating variable missing, {jerr}")
            print(f"Impossible to parse the workflow. Templating variable missing, {jerr}", file=sys.stderr)
            sys.exit(ExitCodes.WORKFLOW_NOT_VALID.value)

    else:
        print("Unsupported workflow format file %s" % cmd_args.workflow, file=sys.stderr)
        sys.exit(ExitCodes.WORKFLOW_FILE_TYPE_NOT_SUPPORTED.value)

    # run the workflow
    if cmd_args.filter_nodes != "":
        filtered_nodes = cmd_args.filter_nodes.split(",")
        aw.set_filtered_nodes(filtered_nodes)
    if cmd_args.skip_nodes != "":
        skipped_nodes = cmd_args.skip_nodes.split(",")
        aw.set_skipped_nodes(skipped_nodes)

    start_from_node = cmd_args.start_from_node if cmd_args.start_from_node else '_s'
    end_to_node = cmd_args.end_to_node if cmd_args.end_to_node else '_e'

    if cmd_args.mode == 'textual' and not cmd_args.verify_only:
        output = TextualWorkflowOutput(workflow=aw, event=threading.Event(), logging_dir=logging_dir, log_level=cmd_args.log_level, cmd_args=cmd_args)
        output.run(start_node=start_from_node, end_node=end_to_node, verify_only=cmd_args.verify_only)
    else:
        output_threads = []
        if cmd_args.mode == 'stdout' or cmd_args.verify_only:
            stdout_thread = StdoutWorkflowOutput(workflow=aw, event=threading.Event(), logging_dir=logging_dir, log_level=cmd_args.log_level, cmd_args=cmd_args)
            stdout_thread.start()
            output_threads.append(stdout_thread)

        if cmd_args.draw_png:
            png_thread = PngDrawflowOutput(workflow=aw, event=threading.Event(), logging_dir=logging_dir, log_level=cmd_args.log_level, cmd_args=cmd_args)
            png_thread.start()
            output_threads.append(png_thread)

        def signal_handler(sig, frame):
            console = Console()
            y_or_n = console.input(' Do you want to quit the software?')
            if not aw.is_stopping():
                while y_or_n.lower() != 'y' and y_or_n.lower() != 'n' :
                    y_or_n = console.input(' Do you want to quit the software? [y/n]')
                if y_or_n == 'y':
                    console.print(" Workflow stopping, please wait that running playbooks end.")
                    aw.stop()


        signal.signal(signal.SIGINT, signal_handler)

        aw.run(start_node=start_from_node, end_node=end_to_node, verify_only=cmd_args.verify_only)

        for output_thread in output_threads:
            output_thread.join()


if __name__ == "__main__":
    main()
