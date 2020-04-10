#!/bin/bash

systemctl stop clusrun
rm -f /etc/systemd/system/clusrun.service

dir=$(dirname "$0")

if [ "${1,,}" == "-cleanup" ]; then
    rm -rf "$dir/clusnode.db" "$dir/clusnode.logs"
    rm -f "$dir/clusnode.config"
fi

rm -f "$dir/clusnode" "$dir/clus"
rm -f "$0"