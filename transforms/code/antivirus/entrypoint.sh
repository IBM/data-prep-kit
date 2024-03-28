#!/usr/bin/env bash

set -euo pipefail

# Start cland
clamd
# Wait for starting up clamd
check="import clamd, time
while True:
  try:
    clamd.ClamdUnixSocket().ping()
    break
  except:
    print('waiting for starting up clamd')
    time.sleep(5)
"
python -c "$check"
# Run commands
exec "$@"
