#!/bin/env python
import argparse
import sys

with open("args", "w") as f:
    f.write("%s" % sys.argv)

parser = argparse.ArgumentParser()
parser.add_argument('--vault-id', action='store', default=None,
                    dest='vault_id',
                    help='name of the vault secret to get from keyring')

parsed = parser.parse_args()
secret = None
if parsed.vault_id == 'V1':
    secret = 'password1'
elif parsed.vault_id == 'V2':
    secret = 'password2'
#  sys.exit(1)
sys.stdout.write('%s\n' % secret)
sys.exit(0)
