#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import argparse
import os
import sys
from datetime import datetime

def check_file_existence(workflow, inventories):
    error = False
    if not os.path.exists(workflow):
        print(f"The workflow file {workflow} doesn't exist. Please provide a correct file")
        error = True

    if inventories:
        inventory_list = inventories.split(',')
        for inventory in inventory_list:
            if not os.path.exists(inventory):
                print(f"The inventory file {inventory} doesn't exist. Please provide a correct file")
                error = True
    if error:
        sys.exit(1)

def main():
    parser = argparse.ArgumentParser(description='Ansible Workflow runner.')
    subparsers = parser.add_subparsers(dest='command', required=True, help='Sub-command help')

    # Server command
    parser_server = subparsers.add_parser('server', help='Start the workflow server')
    parser_server.add_argument('workflow', type=str, help='Workflow file')
    parser_server.add_argument('-i', '--inventory', dest='inventory', required=True,
                               help='specify inventory host path or comma separated host list.')
    parser_server.add_argument('--log-dir', dest='log_dir', default='/tmp/ansible-workflows',
                               help='set the parent output logging directory. defaults to /tmp/ansible-workflows')

    # Client command
    parser_client = subparsers.add_parser('client', help='Start the Textual UI client')

    args = parser.parse_args()

    if args.command == 'server':
        check_file_existence(args.workflow, args.inventory)
        from ansible_workflow.server import start_server
        start_server(args.workflow, args.inventory, args.log_dir)

    elif args.command == 'client':
        from ansible_workflow.client import main as client_main
        client_main()

if __name__ == "__main__":
    main()
