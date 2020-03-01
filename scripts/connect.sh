#!/bin/bash

set -Eeuxo pipefail

SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"

ssh -t gavinsvr "cd ~/ws/presto-minio;/bin/bash"
