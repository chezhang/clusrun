#!/bin/bash

systemctl daemon-reload
systemctl stop clusnode
rm -f /etc/systemd/system/clusnode.service

dir=$(dirname "$0")

if [ "${1,,}" == "-cleanup" ]; then
    rm -rf "$dir/clusnode.db" "$dir/clusnode.logs"
    rm -f "$dir/clusnode.config" "$dir/cert.pem" "$dir/key.pem"
fi

rm -f "$dir/clusnode" "$dir/clus"
rm -f /usr/local/bin/clus /usr/local/bin/clusnode

if [ "${1,,}" == "-cleanup" ]; then
    rm -f "$0"
fi