#!/bin/bash

set -Eeuxo pipefail

SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"

ssh -p 443 -t gavin@10.0.0.2 "cd ~/ws/presto-minio;/bin/bash"
