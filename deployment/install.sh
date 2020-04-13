#!/bin/bash

port=${1:-50505}

systemctl stop clusrun

pushd $(dirname "$0")

cat <<EOF >/etc/systemd/system/clusrun.service
[Unit]
Description=clusrun service

[Service]
User=root
ExecStart=$(pwd)/clusnode start -host localhost:$port
LimitNOFILE=65536

[Install]
WantedBy=multi-user.target
EOF

systemctl daemon-reload
systemctl enable clusrun
systemctl start clusrun

ln -s $(pwd)/clus /usr/local/bin/clus
ln -s $(pwd)/clusnode /usr/local/bin/clusnode

popd