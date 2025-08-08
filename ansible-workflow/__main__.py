#!/usr/bin/env python3
# -*- coding: utf-8 -*-
#
# Copyright 2021 Claudio Papa
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

import threading, time
from workflow import AnsibleWorkflow
from textual_gui import WorkflowApp
import argparse
import os.path
import sys
from datetime import datetime


def read_options():
    parser = argparse.ArgumentParser(description='This programs mimics the AWX/Ansible Tower® workflows from command line.')
    parser.add_argument('workflow', type=str, help='Workflow file')
    parser.add_argument('-i', '--inventory', dest='inventory', required=True,
                        help='specify inventory host path or comma separated host list.')
    parser.add_argument('--no-curses', dest='skip_curses', default=False,
                        action='store_true',
                        help='skip the rendering of the progress using curses.')
    parser.add_argument('--log-dir', dest='log_dir', default='ansible-workflow-logs',
                        help='set the parent output logging directory. defaults to ./ansible-workflow-logs/[workflow name]-[execution time]')

    return parser.parse_args()

def check_file_existence(workflow, inventories):
    error = False
    if not os.path.exists(workflow):
        print("The workflow file %s doesn't exists. Please provide a correct file" % workflow)
        error = True

    inventory_list = inventories.split(',')
    for inventory in inventory_list:
        if not os.path.exists(inventory):
            print("The inventory file %s doesn't exists. Please provide a correct file" % inventory)
            error = True
    if error:
        sys.exit(1)

def main():
    cmd_args = read_options()
    check_file_existence(cmd_args.workflow, cmd_args.inventory)
    logging_dir = "%s/%s_%s" % (cmd_args.log_dir, os.path.basename(cmd_args.workflow),datetime.now().strftime("%Y%m%d_%H%M%S"))
    aw = AnsibleWorkflow(workflow=cmd_args.workflow,
                        inventory=cmd_args.inventory,
                        logging_dir=logging_dir)

    #aw.draw_graph()
    #aw.print_graph()

    #aw.run()
    #stdscr.getch()
    if not cmd_args.skip_curses:
        workflow_thread = threading.Thread(target=aw.run)
        workflow_thread.start()
        app = WorkflowApp()
        app.set_workflow(aw)
        app.run()
    else:
        aw.run()

if __name__ == "__main__":
    main()
    #curses.wrapper(main)
