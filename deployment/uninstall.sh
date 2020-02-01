#!/bin/bash

systemctl stop clusrun
rm -f /etc/systemd/system/clusrun.service

dir=$(dirname "$0")
rm -rf "$dir/clusnode.db" "$dir/clusnode.log"
rm -f "$dir/clusnode" "$dir/clus" "$dir/clusnode.config"
rm -f "$0"