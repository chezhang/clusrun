#!/bin/bash

port=${1:-50505}

systemctl stop clusnode 2>/dev/null

pushd $(dirname "$0")

cat <<EOF >/etc/systemd/system/clusnode.service
[Unit]
Description=clusnode service

[Service]
User=root
ExecStart=$(pwd)/clusnode start -host localhost:$port
LimitNOFILE=65536

[Install]
WantedBy=multi-user.target
EOF

systemctl daemon-reload
systemctl enable clusnode
systemctl start clusnode

ln -s $(pwd)/clus /usr/local/bin/clus
ln -s $(pwd)/clusnode /usr/local/bin/clusnode

popd