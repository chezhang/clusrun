#!/bin/bash

systemctl daemon-reload
systemctl stop clusrun
rm -f /etc/systemd/system/clusrun.service

dir=$(dirname "$0")

if [ "${1,,}" == "-cleanup" ]; then
    rm -rf "$dir/clusnode.db" "$dir/clusnode.logs"
    rm -f "$dir/clusnode.config"
fi

rm -f "$dir/clusnode" "$dir/clus"
rm -f /usr/local/bin/clus /usr/local/bin/clusnode

if [ "${1,,}" == "-cleanup" ]; then
    rm -f "$0"
fi