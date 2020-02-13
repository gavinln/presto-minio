#!/bin/bash

set -Eeuxo pipefail

NET_INTERFACE=enp0s8
IP_ADDRESS=$(ip -4 addr show $NET_INTERFACE | grep -Eo 'inet [0-9\.]+' | sed 's/inet //')

echo "Minio at http://$IP_ADDRESS:9000/"
echo "Presto at http://$IP_ADDRESS:8080/"
